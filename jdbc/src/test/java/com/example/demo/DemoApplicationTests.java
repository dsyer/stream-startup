package com.example.demo;

import java.util.Collections;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import com.example.demo.Event.Type;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.DigestUtils;
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
				value -> value.contains("kafka_offset=4"));
		assertThat(err).doesNotContain("kafka_offset=2");
		assertThat(err).doesNotContain("DONE:");
		Awaitility.await().until(() -> client.offset(Inputs.PENDING), value -> value > 3);
		assertThat(client.max(Event.Type.PENDING)).isGreaterThan(3);
		client.done(4, "bar1");
		err = Awaitility.await().until(output::getErr, value -> value.contains("DONE:"));
		assertThat(client.find(4)).isEqualTo(Event.Type.DONE);
	}

	@TestConfiguration
	public static class KafkaClientConfiguration {

		private final KafkaTemplate<String, byte[]> kafka;
		private final ConsumerFactory<String, byte[]> factory;
		private final JdbcTemplate jdbc;

		public KafkaClientConfiguration(KafkaTemplate<String, byte[]> template,
				ConsumerFactory<String, byte[]> factory, DataSource datasSource) {
			this.jdbc = new JdbcTemplate(datasSource);
			this.kafka = template;
			this.factory = factory;
		}

		public Event.Type find(long id) {
			return this.jdbc.queryForObject("SELECT type FROM event where offset=?",
					Event.Type.class, id);
		}

		public ListenableFuture<SendResult<String, byte[]>> done(long id, String value) {
			return kafka.send(MessageBuilder.withPayload(value.getBytes())
					.setHeader(KafkaHeaders.MESSAGE_KEY,
							DigestUtils.md5Digest(("foo" + id).getBytes()))
					.setHeader(KafkaHeaders.TOPIC, Inputs.DONE).build());
		}

		public Long max(Type type) {
			// TODO: where type=type
			return jdbc.queryForObject("SELECT max(offset) FROM event", Long.class);
		}

		public Long offset(String topic) {
			return jdbc.queryForObject("SELECT max(offset) FROM offset where topic=?",
					Long.class, topic);
		}

		@PostConstruct
		public void init() {
			System.err.println(jdbc.queryForList("SELECT * FROM offset"));
			int count = jdbc.update("UPDATE offset SET offset=? WHERE topic=? AND part=0",
					2, Inputs.PENDING);
			if (count < 1) {
				jdbc.update("INSERT INTO offset (topic, part, offset) values (?,0,?)",
						Inputs.PENDING, 2);
			}
			jdbc.update("DELETE FROM event WHERE offset >= ?", 3);
			try (Consumer<String, byte[]> consumer = factory.createConsumer()) {
				TopicPartition partition = new TopicPartition(Inputs.PENDING, 0);
				consumer.assign(Collections.singleton(partition));
				if (consumer.position(partition) > 2) {
					System.err.println("No need to seed logs");
					return;
				}
			}
			for (int i = 0; i < 5; i++) {
				this.kafka.send(Inputs.PENDING, ("foo" + i).getBytes());
			}
		}

	}

	public static class Initializer
			implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		private static MySQLContainer<?> mysql;

		private static KafkaContainer kafka;

		static {
			kafka = new KafkaContainer().withNetwork(null).withReuse(true);
			mysql = new MySQLContainer<>().withUsername("test").withPassword("test")
					.withDatabaseName("test").withReuse(true);
			Startables.deepStart(Stream.of(kafka, mysql)).join();
		}

		@Override
		public void initialize(ConfigurableApplicationContext context) {
			TestPropertyValues
					.of("spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
							"spring.datasource.url=" + mysql.getJdbcUrl(),
							"spring.datasource.username=" + mysql.getUsername(),
							"spring.datasource.password=" + mysql.getPassword())
					.applyTo(context);
		}

	}
}
