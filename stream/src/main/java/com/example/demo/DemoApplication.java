package com.example.demo;

import com.example.demo.DemoApplication.Events;
import com.example.demo.DemoApplication.Table;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Events.class, Table.class })
public class DemoApplication {

	interface Events {
		String INPUT = "input";
		String EVENTS = "events";
		String OUTPUT = "output";
		String TABLE = "table";
		String DONE = "done";

		@Input(Events.INPUT)
		SubscribableChannel input();

		@Output(Events.EVENTS)
		MessageChannel events();

	}

	interface Table {
		@Input(Events.TABLE)
		GlobalKTable<Long, Event> events();

		@Input(Events.DONE)
		KStream<Long, byte[]> done();
		@Output(Events.OUTPUT)
		KStream<Long, Event> output();
	}

	public static final String EVENT_ID = "x_event_id";

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
					.setHeader(KafkaHeaders.MESSAGE_KEY, ("" + offset).getBytes())
					.build();
		}
		return null;
	}

	@StreamListener(value = Events.DONE)
	@SendTo(Events.OUTPUT)
	public KStream<Long, Event> done(KStream<Long, byte[]> messages) {
		return messages.map((id, payload) -> {
			System.err.println("DONE: " + id);
			return new KeyValue<>(id, new Event(id, payload, Event.Type.DONE));
		});
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
