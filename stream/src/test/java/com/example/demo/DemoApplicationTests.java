package com.example.demo;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;

import com.example.demo.DemoApplication.Events;
import com.example.demo.Event.Type;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.util.TestPropertyValues;
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
		Awaitility.await().until(() -> client.max(), value -> value > 3);
		assertThat(client.max(Event.Type.PENDING)).isGreaterThan(3);
		client.done(4, "bar1");
		err = Awaitility.await().until(output::getErr, value -> value.contains("DONE:"));
		assertThat(client.find(4)).isEqualTo(Type.PENDING);
	}

	@TestConfiguration
	public static class KafkaClientConfiguration {

		private final KafkaTemplate<Long, byte[]> kafka;
		private final ConsumerFactory<Long, byte[]> factory;
		private InteractiveQueryService interactiveQueryService;

		ReadOnlyKeyValueStore<Long, Event> store;

		public KafkaClientConfiguration(KafkaTemplate<Long, byte[]> template,
				ConsumerFactory<Long, byte[]> factory,
				InteractiveQueryService interactiveQueryService)
				throws InterruptedException, ExecutionException {
			this.kafka = template;
			this.factory = factory;
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

		public ListenableFuture<SendResult<Long, byte[]>> done(long id, String value) {
			// ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(id);
			return kafka.send(MessageBuilder.withPayload(value.getBytes())
					.setHeader(KafkaHeaders.MESSAGE_KEY, id)
					.setHeader(KafkaHeaders.TOPIC, Events.DONE).build());
		}

		public Long max(Type type) {
			try {
				if (store == null) {
					store = interactiveQueryService.getQueryableStore(Events.EVENTSTORE,
							QueryableStoreTypes.keyValueStore());
				}
				final KeyValueIterator<Long, Event> all = store.all();

				Iterable<KeyValue<Long, Event>> t = () -> all;
				final long max = StreamSupport.stream(t.spliterator(), false)
						.filter(k -> k.value.getType() == type).mapToLong(k -> k.key)
						.max().orElse(-1L);
				return max;
			}
			catch (Exception e) {
				e.printStackTrace();
				return -1L;
			}
		}

		public Long max() {
			try {
				if (store == null) {
					store = interactiveQueryService.getQueryableStore(Events.EVENTSTORE,
							QueryableStoreTypes.keyValueStore());
				}
				final KeyValueIterator<Long, Event> all = store.all();
				Iterable<KeyValue<Long, Event>> t = () -> all;
				final long max = StreamSupport.stream(t.spliterator(), false)
						.mapToLong(k -> k.key).max().orElse(-1L);
				return max;
			}
			catch (Exception e) {
				e.printStackTrace();
				return -1L;
			}
		}

		@PostConstruct
		public void init() {
			try (Consumer<Long, byte[]> consumer = factory.createConsumer()) {
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
