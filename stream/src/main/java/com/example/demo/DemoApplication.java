package com.example.demo;

import java.nio.ByteBuffer;

import com.example.demo.DemoApplication.Events;
import com.example.demo.DemoApplication.Tables;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import org.springframework.util.Base64Utils;

@SpringBootApplication(proxyBeanMethods = false)
@EnableBinding({ Events.class, Tables.class, Inputs.class })
public class DemoApplication {

	interface Events {
		String EVENTS = "events";
		String MORE = "more";

		@Output(EVENTS)
		KStream<byte[], Event> events();

		@Output(MORE)
		KStream<byte[], Event> more();

	}

	interface Tables {
		String EVENTS = "event-store";
		String MORE = "more-store";

		@Input(EVENTS)
		KTable<byte[], Event> eventsTable();

		@Input(MORE)
		KTable<byte[], Event> moreTable();

	}

	private final KeyExtractor extractor;

	public DemoApplication(KeyExtractor extractor) {
		this.extractor = extractor;
	}

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@StreamListener
	@SendTo(Events.EVENTS)
	public KStream<byte[], Event> input(
			@Input(Inputs.PENDING) KStream<byte[], byte[]> messages,
			@Input(Tables.EVENTS) KTable<byte[], Event> events) {
		return messages //
				.selectKey((k, v) -> {
					System.err.println("Incoming: " + new String(v));
					return extractor.extract(k, v);
				}) //
				.leftJoin(events, (value, event) -> new KeyValue<>(value, event)) //
				.filter((k, v) -> {
					if (v.value != null) {
						System.err.println("EXISTS: " + v.value);
					}
					return v.value == null;
				}) //
				.map((k, v) -> new KeyValue<>(k, v.key)) //
				.transform(
						() -> new Transformer<byte[], byte[], KeyValue<byte[], Event>>() {

							private ProcessorContext context;

							@Override
							public void init(ProcessorContext context) {
								this.context = context;
							}

							@Override
							public KeyValue<byte[], Event> transform(byte[] key,
									byte[] value) {
								System.err.println("TRANSFORM: " + new Event(
										context.offset(), key, Event.Type.UNKNOWN));
								return new KeyValue<>(key, new Event(context.offset(),
										key, Event.Type.UNKNOWN));
							}

							@Override
							public void close() {
							}
						})
				.map((key, v) -> {
					System.err.println("PENDING: " + v);
					Long offset = v.getOffset();
					return new KeyValue<>(key,
							new Event(offset, key, Event.Type.PENDING));
				});
	}

	static byte[] getBytes(Long offset) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(offset);
		return buffer.array();
	}

}

@Component
class DoneListener {

	@StreamListener
	@SendTo(Events.MORE)
	public KStream<byte[], Event> done(
			@Input(Inputs.DONE) KStream<byte[], byte[]> messages,
			@Input(Tables.MORE) KTable<byte[], Event> events) {
		return messages //
				.leftJoin(events, (value, event) -> new KeyValue<>(value, event)) //
				.filter((k, v) -> v.value != null
						&& v.value.getType() == Event.Type.PENDING)
				.map((key, v) -> {
					Long offset = v.value.getOffset();
					Event value = new Event(offset, key, Event.Type.DONE);
					System.err.println(
							"DONE: " + Base64Utils.encodeToString(key) + ", " + value);
					return new KeyValue<>(key, value);
				});
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
		return "Event [offset=" + offset + ", value=["
				+ Base64Utils.encodeToString(this.value) + "], type=" + type + "]";
	}

}
