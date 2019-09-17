package com.example.demo;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;

import com.example.demo.DemoApplication.Events;
import com.example.demo.Event.Type;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.cloud.stream.annotation.Input;
import org.testcontainers.containers.KafkaContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.concurrent.ListenableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@ExtendWith(OutputCaptureExtension.class)
@ContextConfiguration(initializers = DemoApplicationTests.Initializer.class)
public class DemoApplicationTests {

	@Autowired
	private KafkaClientConfiguration client;


	@Test
	public void contextLoads(CapturedOutput output) throws Exception {
		String err = Awaitility.await().until(output::getErr,
				value -> value.contains("kafka_offset=3"));
		// assertThat(err).doesNotContain("kafka_offset=2");
		assertThat(err).doesNotContain("DONE:");
		Awaitility.await().until(() -> client.offset(Events.EVENTS), value -> value > 3);
		assertThat(client.max(Event.Type.PENDING)).isGreaterThan(3);
		client.done(4, "bar1");
		err = Awaitility.await().until(output::getErr, value -> value.contains("DONE:"));
		assertThat(client.find(4)).isEqualTo(Type.PENDING);
	}

	@TestConfiguration
	public static class KafkaClientConfiguration {

		private final KafkaTemplate<String, byte[]> kafka;
		private final ConsumerFactory<String, byte[]> factory;
		private InteractiveQueryService interactiveQueryService;
		private GlobalKTable<Long, Event> events;

		ReadOnlyKeyValueStore<Long, Event> store;

		public KafkaClientConfiguration(KafkaTemplate<String, byte[]> template,
										ConsumerFactory<String, byte[]> factory,
										InteractiveQueryService interactiveQueryService)
				throws InterruptedException, ExecutionException {
			this.kafka = template;
			this.factory = factory;
			this.interactiveQueryService = interactiveQueryService;
		}

		public Event.Type find(long id) {
			return Event.Type.PENDING;
		}

		public ListenableFuture<SendResult<String, byte[]>> done(long id, String value) {
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.putLong(id);

			return kafka.send(MessageBuilder.withPayload(value.getBytes())
					.setHeader(KafkaHeaders.MESSAGE_KEY, buffer.array())
					.setHeader(KafkaHeaders.TOPIC, "done").build());
		}

		public Long max(Type type) {
			// TODO: where type=type
			return 4L;
		}

		@StreamListener(Events.TABLE)
		public void bind(GlobalKTable<Long, Event> events) {
			System.err.println("STORE: " + events.queryableStoreName());
			this.events = events;
		}

		// The following StreamListener is equivalent in functionality to the above one where we are binding to a GlobalKTable.
		// If you are uncommenting this method, make sure to uncomment the above StreamListener method and
		// remove the corresponding materializedAs property from application.properties. In addition, you need to
		// add a new binding as below.
		//      @Input("stream")
		//		KStream<Long, Event> stream();
		// and then provide spring.cloud.stream.bindings.stream.destination=events

//		@StreamListener
//		public void bind(@Input("stream") KStream<Long, Event> events) {
//			events.groupByKey().reduce((value1, value2) -> value2, Materialized.as("events-x"));
//		}

		public Long offset(String topic) {
			try {
				//System.err.println("QUERY: " + events.queryableStoreName());
				if (store == null) {
					store = interactiveQueryService
							.getQueryableStore("events-x",
									QueryableStoreTypes.keyValueStore());
				}
				//Event event = store.get(1L);
				final KeyValueIterator<Long, Event> all = store.all();

				Iterable<KeyValue<Long, Event>> t = () -> all;
				final long max = StreamSupport.stream(t.spliterator(), false).mapToLong(k -> k.key).max()
						.orElseThrow(NoSuchElementException::new);
				//System.err.println("EVENT: " + store.all().hasNext());
				return max;
			}
			catch (Exception e) {
				e.printStackTrace();
				System.err.println("WAH");
				return -1L;
			}
		}

		@PostConstruct
		public void init() {
			try (Consumer<String, byte[]> consumer = factory.createConsumer()) {
				TopicPartition partition = new TopicPartition("input", 0);
				consumer.assign(Collections.singleton(partition));
				if (consumer.position(partition) > 2) {
					System.err.println("No need to seed logs");
					return;
				}
			}
			for (int i = 0; i < 5; i++) {
				this.kafka.send("input", ("foo" + i).getBytes());
			}
		}

	}

	public static class Initializer
			implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		private static KafkaContainer kafka;

		static {
			kafka = new KafkaContainer().withNetwork(null).withReuse(true);
			kafka.start();
		}

		@Override
		public void initialize(ConfigurableApplicationContext context) {
			TestPropertyValues
					.of("spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers())
					.applyTo(context);
		}

	}
}
