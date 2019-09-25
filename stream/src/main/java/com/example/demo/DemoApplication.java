package com.example.demo;

import java.nio.ByteBuffer;

import com.example.demo.DemoApplication.Events;
import com.example.demo.DemoApplication.Tables;
import com.example.demo.Event.Type;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Events.class, Tables.class })
public class DemoApplication {

	interface Events {
		String INPUT = "input";
		String AUDIT = "audit";
		String AUDITSTORE = "audit-store";
		String EVENTS = "events";
		String EVENTSTORE = "event-store";
		String DONE = "done";

		@Input(INPUT)
		SubscribableChannel input();

		@Output(AUDIT)
		MessageChannel audit();

		@Output(EVENTS)
		MessageChannel events();

		@Input(DONE)
		SubscribableChannel done();

	}

	interface Tables {
		String EVENTS = "tmp-events";
		String AUDIT = "tmp-audit";

		@Input(EVENTS)
		KStream<Long, Event> eventsTable();

		@Input(AUDIT)
		// TODO: better byte[]
		KStream<byte[], byte[]> auditTable();
	}

	private final EventService service;

	private final Events events;

	private final AuditService audit;

	private final KeyExtractor extractor;

	public DemoApplication(EventService service, AuditService audit, KeyExtractor extractor, Events events) {
		this.service = service;
		this.audit = audit;
		this.extractor = extractor;
		this.events = events;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@StreamListener(value = Events.INPUT)
	public void input(Message<byte[]> message) {
		System.err.println("PENDING: " + message);
		byte[] key = extractIdentifier(message);
		if (audit.exists(key)) {
			System.err.println("EXISTS: " + key);
			return;
		}
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		if (offset != null) {
			final byte[] longBytes = getBytes(offset);

			System.err.println("SENDING: " + offset + ", " + key);
			events.audit().send(MessageBuilder.withPayload(message.getPayload())
					.setHeader(KafkaHeaders.MESSAGE_KEY, longBytes).build());
			events.events().send(MessageBuilder
					.withPayload(
							new Event(offset, message.getPayload(), Event.Type.PENDING))
					.setHeader(KafkaHeaders.MESSAGE_KEY, longBytes).build());
		}

	}

	private byte[] extractIdentifier(Message<byte[]> message) {
		return extractor.extract(message);
	}

	static byte[] getBytes(Long offset) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(offset);
		return buffer.array();
	}

	@StreamListener(value = Events.DONE)
	@SendTo(Events.EVENTS)
	public Message<?> done(Message<byte[]> message) {
		System.err.println("DONE: " + message);
		Long id = getLongHeader(message);
		System.err.println("DONE: " + id);
		Type type = service.find(id);
		if (type != Type.PENDING) {
			System.err.println("Not processed: " + id + " with type=" + type);
			return null;
		}
		final byte[] longBytes = getBytes(id);
		return MessageBuilder
				.withPayload(new Event(id, message.getPayload(), Event.Type.DONE))
				.setHeader(KafkaHeaders.MESSAGE_KEY, longBytes).build();
	}

	private Long getLongHeader(Message<byte[]> message) {
		// Postel's Law: be conservative in what you accept
		Object key = extractIdentifier(message);
		if (key instanceof Long) {
			System.err.println("LONG: " + key);
			return (Long) key;
		}
		if (key instanceof byte[]) {
			ByteBuffer buffer = ByteBuffer.wrap((byte[]) key);
			System.err.println("BYTES: " + key);
			return (Long) buffer.asLongBuffer().get();
		}
		if (key instanceof ByteBuffer) {
			System.err.println("BUFFER: " + key);
			ByteBuffer buffer = (ByteBuffer) key;
			return (Long) buffer.asLongBuffer().get();
		}
		return -1L;
	}

	@StreamListener
	public void bind(@Input(Tables.EVENTS) KStream<Long, Event> events,
			@Input(Tables.AUDIT) KStream<byte[], byte[]> input) {
		events.groupByKey().reduce((id, event) -> event,
				Materialized.as(Events.EVENTSTORE));
		input.groupByKey().reduce((id, data) -> data, Materialized.as(Events.AUDITSTORE));
	}

}

@Component
class EventService {
	private final InteractiveQueryService interactiveQueryService;
	private ReadOnlyKeyValueStore<Long, Event> store;

	public EventService(InteractiveQueryService interactiveQueryService) {
		this.interactiveQueryService = interactiveQueryService;
	}

	public Event.Type find(long id) {
		try {
			if (store == null) {
				store = interactiveQueryService.getQueryableStore(Events.EVENTSTORE,
						QueryableStoreTypes.keyValueStore());
			}
			Event event = store.get(id);
			if (event == null) {
				return Event.Type.UNKNOWN;
			}
			return event.getType();
		}
		catch (Exception e) {
			e.printStackTrace();
			return Event.Type.UNKNOWN;
		}
	}

}

@Component
class AuditService {
	private final InteractiveQueryService interactiveQueryService;
	private ReadOnlyKeyValueStore<byte[], byte[]> store;

	public AuditService(InteractiveQueryService interactiveQueryService) {
		this.interactiveQueryService = interactiveQueryService;
	}

	public boolean exists(Object id) {
		try {
			if (store == null) {
				store = interactiveQueryService.getQueryableStore(Events.AUDITSTORE,
						QueryableStoreTypes.keyValueStore());
			}
			if (id == null) {
				return false;
			}
			if (id instanceof byte[]) {
				byte[] data = store.get((byte[]) id);
				if (data == null) {
					return false;
				}
				return true;
			}
			else {
				throw new IllegalStateException("Wrong key type: " + id);
			}
		}
		catch (IllegalStateException e) {
			throw e;
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

}

class Event {

	enum Type {
		PENDING, DONE, CANCELLED, UNKNOWN;
	}

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
