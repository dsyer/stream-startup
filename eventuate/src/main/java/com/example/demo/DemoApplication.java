package com.example.demo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.transaction.Transactional;

import io.eventuate.tram.events.common.DomainEvent;
import io.eventuate.tram.events.publisher.DomainEventPublisher;
import io.eventuate.tram.events.publisher.TramEventsPublisherConfiguration;
import io.eventuate.tram.events.subscriber.TramEventSubscriberConfiguration;
import io.eventuate.tram.jdbckafka.TramJdbcKafkaConfiguration;
import org.apache.kafka.common.TopicPartition;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Example;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.Base64Utils;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Inputs.class })
@Import({ TramJdbcKafkaConfiguration.class, TramEventsPublisherConfiguration.class,
		TramEventSubscriberConfiguration.class })
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
	private final OffsetRepository offsets;
	private final DomainEventPublisher domainEventPublisher;

	public EventService(DomainEventPublisher domainEventPublisher, EventRepository events, OffsetRepository offsets) {
		this.domainEventPublisher = domainEventPublisher;
		this.offsets = offsets;
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
		offsets.save(new Offset(Inputs.PENDING, 0L, offset));
		Event event = events.save(new Event(offset, key, Event.Type.PENDING));
		domainEventPublisher.publish(Event.class, key, Arrays.asList(event));
	}

	@Transactional
	public void complete(Long offset, byte[] key) {
		Optional<Event> event = events
				.findOne(Example.of(new Event(null, key, Event.Type.PENDING)));
		System.err.println("Saving DONE offset=" + offset);
		offsets.save(new Offset(Inputs.DONE, 0L, offset));
		if (!event.isPresent()) {
			System.err
					.println("Not updating Event key=" + Base64Utils.encodeToString(key));
			return;
		}
		System.err.println("Updating Event key=" + Base64Utils.encodeToString(key));
		Event publish = events.save(new Event(event.get().getOffset(), event.get().getHash(),
				Event.Type.DONE));
		domainEventPublisher.publish(Event.class, key, Arrays.asList(publish));
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
class Event implements DomainEvent {

	enum Type {
		PENDING, DONE, CANCELLED, UNKNOWN;
	}

	@Id
	private Long offset;

	private Type type = Type.PENDING;

	private String hash;

	public Event() {
	}

	public Event(Long offset, byte[] hash, Event.Type type) {
		this(offset, Base64Utils.encodeToString(hash), type);
	}

	public Event(Long offset, String hash, Event.Type type) {
		this.offset = offset;
		this.hash = hash;
		this.type = type;
	}

	public String getHash() {
		return this.hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
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
		return "Event [offset=" + offset + ", hash=[" + this.hash + "], type=" + type
				+ "]";
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
		Offset offset = offsets.findById(new OffsetId(topic, 0L))
				.orElseGet(() -> offsets.save(new Offset(topic, 0L, 0L)));
		this.cache.put(topic, offset.getOffset());
	}

}
