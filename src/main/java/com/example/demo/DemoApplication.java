package com.example.demo;

import java.util.Collection;
import java.util.function.Consumer;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.sql.DataSource;
import javax.transaction.Transactional;

import org.apache.kafka.common.TopicPartition;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@SpringBootApplication(proxyBeanMethods = false)
public class DemoApplication {

	private final EventService service;

	public DemoApplication(EventService service) {
		this.service = service;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@Bean
	public Consumer<Message<byte[]>> consumer() {
		return message -> {
			Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
			System.err.println(message);
			if (offset != null) {
				service.add(offset, message.getPayload());
			}
		};
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
									long offset = config.getOffset(partition.topic(), partition.partition());
									System.err.println("Seeking: " + partition
											+ " to offset=" + offset);
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
		System.err.println("Saving offset=" + offset);
		template.update("UPDATE offsets SET offset=? WHERE id=1", offset);
		events.save(new Event(offset, data));
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

	public Event(long offset, byte[] value) {
		this.offset = offset;
		this.value = value;
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

	@Override
	public String toString() {
		return "Event [offset=" + offset + ", value=[" + this.value.length + "], type="
				+ type + "]";
	}

}

class ConsumerConfiguration {
	private long offset = 0;
	private JdbcTemplate template;
	private boolean initialized = false;

	public ConsumerConfiguration(DataSource datasSource) {
		this.template = new JdbcTemplate(datasSource);
	}

	public long getOffset(String topic, int partition) {
		init();
		return this.offset;
	}

	private void init() {
		if (this.initialized) {
			return;
		}
		this.offset = this.template
				.queryForObject("SELECT offset FROM offsets where id=1", Long.class);
		this.initialized = true;
	}

}
