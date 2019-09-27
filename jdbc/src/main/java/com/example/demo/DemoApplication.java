package com.example.demo;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.sql.DataSource;
import javax.transaction.Transactional;

import org.apache.kafka.common.TopicPartition;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Example;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.Base64Utils;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Inputs.class })
public class DemoApplication {

	private final EventService service;

	private final KeyExtractor extractor;

	public DemoApplication(EventService service, KeyExtractor extractor) {
		this.service = service;
		this.extractor = extractor;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@StreamListener(value = Inputs.PENDING)
	public void input(Message<byte[]> message) {
		byte[] key = extractor.extract(message);
		if (service.exists(key)) {
			System.err.println("PENDING: " + message);
			System.err.println("EXISTS: " + Base64Utils.encodeToString(key));
			return;
		}
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		System.err.println("PENDING: " + message);
		if (offset != null) {
			System.err.println("SENDING: " + offset + ", "
					+ (key == null ? key : Base64Utils.encodeToString(key)));
			service.add(offset, key);
		}
	}

	@StreamListener(value = Inputs.DONE)
	public void done(Message<byte[]> message) {
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		byte[] key = (byte[]) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		System.err.println("DONE: " + message);
		if (offset != null) {
			service.complete(offset, key);
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
	ConsumerConfiguration config(OffsetRepository offsets) {
		return new ConsumerConfiguration(offsets);
	}

}

@Component
class EventService {

	private final EventRepository events;
	private final JdbcTemplate jdbc;

	public EventService(EventRepository events, DataSource dataSource) {
		this.jdbc = new JdbcTemplate(dataSource);
		this.events = events;
	}

	public boolean exists(byte[] key) {
		return false;
	}

	@Transactional
	public void add(Long offset, byte[] key) {
		if (events.existsById(offset)) {
			return;
		}
		System.err.println("Saving PENDING offset=" + offset);
		jdbc.update("UPDATE offset SET offset=? WHERE topic=? AND part=0", offset,
				Inputs.PENDING);
		events.save(new Event(offset, key, Event.Type.PENDING));
	}

	@Transactional
	public void complete(Long offset, byte[] key) {
		Optional<Event> event = events
				.findOne(Example.of(new Event(null, key, Event.Type.PENDING)));
		System.err.println("Saving DONE offset=" + offset);
		jdbc.update("UPDATE offset SET offset=? WHERE topic=? AND part=0", offset,
				Inputs.DONE);
		if (!event.isPresent()) {
			System.err
					.println("Not updating Event key=" + Base64Utils.encodeToString(key));
			return;
		}
		System.err.println("Updating Event key=" + Base64Utils.encodeToString(key));
		events.save(new Event(event.get().getOffset(), event.get().getValue(),
				Event.Type.DONE));
	}
}

interface EventRepository extends JpaRepository<Event, Long> {
}

interface OffsetRepository extends JpaRepository<Offset, OffsetId> {
}

@Entity
@IdClass(OffsetId.class)
class Offset {
	@Id
	private String topic;
	@Id
	private Long part;
	private Long offset;

	Offset() {
	}

	public Offset(String topic, Long partition, Long offset) {
		this.topic = topic;
		this.part = partition;
		this.offset = offset;
	}

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Long getPart() {
		return this.part;
	}

	public void setPart(Long part) {
		this.part = part;
	}

	public Long getOffset() {
		return this.offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}
}

@SuppressWarnings({ "serial", "unused" })
class OffsetId implements Serializable {
	private String topic;
	private Long part;

	OffsetId() {
	}

	public OffsetId(String topic, Long part) {
		this.topic = topic;
		this.part = part;
	}
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

	public Event(Long offset, byte[] value, Event.Type type) {
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
	private Map<String, Long> cache = new HashMap<>();
	private OffsetRepository offsets;

	public ConsumerConfiguration(OffsetRepository offsets) {
		this.offsets = offsets;
	}

	public long getOffset(String topic, int partition) {
		init(topic);
		return this.cache.get(topic);
	}

	private void init(String topic) {
		Long initialized = this.cache.get(topic);
		if (initialized != null) {
			return;
		}
		Offset offset = offsets.findById(new OffsetId(topic, 0L)).orElseGet(() -> {
			Offset value = new Offset(topic, 0L, 0L);
			offsets.save(value);
			return value;
		});
		this.cache.put(topic, offset.getOffset());
	}

}
