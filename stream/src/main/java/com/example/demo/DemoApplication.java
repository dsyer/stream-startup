package com.example.demo;

import com.example.demo.DemoApplication.Events;
import com.example.demo.DemoApplication.Table;
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
@EnableBinding({ Events.class, Table.class })
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

	interface Table {
		String TMP = "tmp";

		@Output(TMP)
		KStream<Long, Event> tmp();
	}

	private final EventService events;

	public DemoApplication(EventService events) {
		this.events = events;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@StreamListener(value = Events.INPUT)
	@SendTo(Events.EVENTS)
	public Message<?> input(Message<byte[]> message) {
		Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
		System.err.println("PENDING: " + message);
		if (offset != null) {
			return MessageBuilder
					.withPayload(
							new Event(offset, message.getPayload(), Event.Type.PENDING))
					.setHeader(KafkaHeaders.MESSAGE_KEY, offset).build();
		}
		return null;
	}

	@StreamListener(value = Events.DONE)
	@SendTo(Events.EVENTS)
	public Message<?> done(Message<byte[]> message) {
		Long id = (Long) message.getHeaders().get(KafkaHeaders.MESSAGE_KEY);
		System.err.println("PENDING: " + message);
		System.err.println("DONE: " + id);
		Type type = events.find(id);
		if (type != Type.PENDING) {
			System.err.println("Not processed: " + id + " with type=" + type);
			return null;
		}
		return MessageBuilder
				.withPayload(new Event(id, message.getPayload(), Event.Type.DONE))
				.setHeader(KafkaHeaders.MESSAGE_KEY, id).build();
	}

	@StreamListener
	public void bind(@Input(Table.TMP) KStream<Long, Event> events) {
		events.groupByKey().reduce((id, event) -> event,
				Materialized.as(Events.EVENTSTORE));
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
