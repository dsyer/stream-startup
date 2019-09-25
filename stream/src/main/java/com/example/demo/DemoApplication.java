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
import org.springframework.util.Base64Utils;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Events.class, Tables.class })
public class DemoApplication {

	interface Events {
		String INPUT = "input";
		String EVENTS = "events";
		String EVENTSTORE = "event-store";
		String DONE = "done";

		@Input(INPUT)
		SubscribableChannel input();

		@Output(EVENTS)
		MessageChannel events();

		@Input(DONE)
		SubscribableChannel done();

	}

	interface Tables {
		String EVENTS = "tmp-events";

		@Input(EVENTS)
		KStream<Long, Event> eventsTable();

	}

	private final EventService service;

	private final AuditService audit;

	private final KeyExtractor extractor;

	public DemoApplication(EventService service, AuditService audit,
			KeyExtractor extractor) {
		this.service = service;
		this.audit = audit;
		this.extractor = extractor;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@StreamListener(value = Events.INPUT)
	@SendTo(Events.EVENTS)
	public Message<?> input(Message<byte[]> message) {
		byte[] key = extractor.extract(message);
		if (audit.exists(key)) {
			System.err.println("PENDING: " + message);
			System.err.println("EXISTS: " + Base64Utils.encodeToString(key));
			return null;
		}
		System.err.println("PENDING: " + message);
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		if (offset != null) {
			System.err.println("SENDING: " + offset + ", "
					+ (key == null ? key : Base64Utils.encodeToString(key)));
			return MessageBuilder.withPayload(new Event(offset, key, Event.Type.PENDING))
					.setHeader(KafkaHeaders.MESSAGE_KEY, key).build();
		}
		return null;
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
		byte[] id = getBytesHeader(message);
		System.err.println("DONE: " + Base64Utils.encodeToString(id));
		Event type = service.find(id);
		if (type.getType() != Type.PENDING) {
			System.err.println("Not processed: " + Base64Utils.encodeToString(id)
					+ " with type=" + type);
			return null;
		}
		return MessageBuilder
				.withPayload(new Event(type.getOffset(), id, Event.Type.DONE))
				.setHeader(KafkaHeaders.MESSAGE_KEY, id).build();
	}

	private byte[] getBytesHeader(Message<byte[]> message) {
		// Postel's Law: be conservative in what you accept
		Object key = message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		if (key instanceof Long) {
			System.err.println("LONG: " + key);
			return getBytes((Long) key);
		}
		if (key instanceof byte[]) {
			System.err.println("BYTES: "
					+ (key == null ? key : Base64Utils.encodeToString((byte[]) key)));
			return (byte[]) key;
		}
		if (key instanceof ByteBuffer) {
			System.err.println("BUFFER: " + key);
			ByteBuffer buffer = (ByteBuffer) key;
			return buffer.array();
		}
		return new byte[0];
	}

	@StreamListener
	public void bind(@Input(Tables.EVENTS) KStream<byte[], Event> events) {
		events.groupByKey().reduce((id, event) -> event,
				Materialized.as(Events.EVENTSTORE));
	}

}

@Component
class EventService {
	private final InteractiveQueryService interactiveQueryService;
	private ReadOnlyKeyValueStore<byte[], Event> store;

	public EventService(InteractiveQueryService interactiveQueryService) {
		this.interactiveQueryService = interactiveQueryService;
	}

	public Event find(byte[] id) {
		try {
			if (store == null) {
				store = interactiveQueryService.getQueryableStore(Events.EVENTSTORE,
						QueryableStoreTypes.keyValueStore());
			}
			Event event = store.get(id);
			if (event == null) {
				return Event.UNKNOWN;
			}
			return event;
		}
		catch (Exception e) {
			e.printStackTrace();
			return Event.UNKNOWN;
		}
	}

}

@Component
class AuditService {
	private final InteractiveQueryService interactiveQueryService;
	private ReadOnlyKeyValueStore<byte[], Event> store;

	public AuditService(InteractiveQueryService interactiveQueryService) {
		this.interactiveQueryService = interactiveQueryService;
	}

	public boolean exists(Object id) {
		try {
			if (store == null) {
				store = interactiveQueryService.getQueryableStore(Events.EVENTSTORE,
						QueryableStoreTypes.keyValueStore());
			}
			if (id == null) {
				return false;
			}
			if (id instanceof byte[]) {
				Event data = store.get((byte[]) id);
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

	public static final Event UNKNOWN = new Event(-1L, new byte[0], Type.UNKNOWN);

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
