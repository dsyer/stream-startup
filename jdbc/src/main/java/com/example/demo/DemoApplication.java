package com.example.demo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.sql.DataSource;
import javax.transaction.Transactional;

import com.example.demo.DemoApplication.Done;
import org.apache.kafka.common.TopicPartition;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.stereotype.Component;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Sink.class, Done.class })
public class DemoApplication {

	interface Done {
		String INPUT = "done";

		@Input(Done.INPUT)
		SubscribableChannel input();
	}

	public static final String EVENT_ID = "x_event_id";

	private final EventService service;

	public DemoApplication(EventService service) {
		this.service = service;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@StreamListener(value = Sink.INPUT)
	public void input(Message<byte[]> message) {
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		System.err.println("PENDING: " + message);
		if (offset != null) {
			service.add(offset, message.getPayload());
		}
	}

	@StreamListener(value = Done.INPUT)
	public void done(Message<byte[]> message) {
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		System.err.println("DONE: " + message);
		if (offset != null) {
			Long id = (Long) message.getHeaders().get(DemoApplication.EVENT_ID);
			if (id != null) {
				service.complete(offset, id, message.getPayload());
			}
			else {
				System.err.println(
						"Error: no event id for incoming data at: offset=" + offset);
			}
		}
	}

	@Bean
	public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> listenerCustomizer(
			ConsumerConfiguration config) {
		return new ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>>() {

			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container,
					String destinationName, String group) {
				System.err.println("Customizing: " + destinationName);
				container.getContainerProperties().setConsumerRebalanceListener(
						new ConsumerAwareRebalanceListener() {
							@Override
							public void onPartitionsAssigned(
									org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
									Collection<TopicPartition> partitions) {
								for (TopicPartition partition : partitions) {
									long offset = config.getOffset(partition.topic(),
											partition.partition()) + 1;
									System.err.println("Seeking: " + partition
											+ " to offset=" + offset + " from position="
											+ consumer.position(partition));
									consumer.seek(partition, offset);
								}
							}
						});
			}
		};
	}

	@Bean
	ConsumerConfiguration config(DataSource datasSource) {
		return new ConsumerConfiguration(datasSource);
	}

}

@Component
class EventService {

	private final EventRepository events;
	private JdbcTemplate template;

	public EventService(EventRepository events, DataSource datasSource) {
		this.template = new JdbcTemplate(datasSource);
		this.events = events;
	}

	@Transactional
	public void add(Long offset, byte[] data) {
		if (events.existsById(offset)) {
			return;
		}
		System.err.println("Saving PENDING offset=" + offset);
		template.update("UPDATE offsets SET offset=? WHERE topic='input' AND part=0",
				offset);
		events.save(new Event(offset, data, Event.Type.PENDING));
	}

	@Transactional
	public void complete(Long offset, Long id, byte[] data) {
		Optional<Event> event = events.findById(id);
		System.err.println("Saving DONE offset=" + offset);
		template.update("UPDATE offsets SET offset=? WHERE topic='done' AND part=0",
				offset);
		if (!event.filter(e -> e.getType() == Event.Type.PENDING).isPresent()) {
			System.err.println("Not updating Event=" + event);
			return;
		}
		events.save(new Event(id, data, Event.Type.DONE));
	}
}

interface EventRepository extends JpaRepository<Event, Long> {
}

@Entity
class Event {

	enum Type {
		PENDING, DONE, CANCELLED, UNKNOWN;
	}

	@Id
	private Long offset;

	private Type type = Type.PENDING;

	private byte[] value;

	public Event() {
	}

	public Event(long offset, byte[] value, Event.Type type) {
		this.offset = offset;
		this.value = value;
		this.type = type;
	}

	public byte[] getValue() {
		return this.value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public Type getType() {
		return this.type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Long getOffset() {
		return this.offset;
	}

	@Override
	public String toString() {
		return "Event [offset=" + offset + ", value=[" + this.value.length + "], type="
				+ type + "]";
	}

}

class ConsumerConfiguration {
	private JdbcTemplate template;
	private Map<String, Long> offset = new HashMap<>();

	public ConsumerConfiguration(DataSource datasSource) {
		this.template = new JdbcTemplate(datasSource);
	}

	public long getOffset(String topic, int partition) {
		init(topic);
		return this.offset.get(topic);
	}

	private void init(String topic) {
		Long initialized = this.offset.get(topic);
		if (initialized != null) {
			return;
		}
		this.offset.put(topic,
				this.template.queryForObject(
						"SELECT offset FROM offsets where topic=? AND part=0",
						Long.class, topic));
	}

}
