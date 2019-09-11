package com.example.demo;

import java.util.Collections;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@ExtendWith(OutputCaptureExtension.class)
@ContextConfiguration(initializers = DemoApplicationTests.Initializer.class)
public class DemoApplicationTests {

	@Autowired
	private JdbcTemplate jdbc;

	@Test
	public void contextLoads(CapturedOutput output) throws Exception {
		String err = Awaitility.await().until(output::getErr,
				value -> value.contains("kafka_offset=3"));
		assertThat(err).doesNotContain("kafka_offset=2");
		Awaitility.await().until(() -> jdbc.queryForObject("SELECT offset FROM offsets WHERE id=1", Long.class),
				value -> value > 3);
		assertThat(jdbc.queryForObject("SELECT max(offset) FROM event", Long.class)).isGreaterThan(3);
	}

	@TestConfiguration
	public static class KafkaClientConfiguration {

		private final KafkaTemplate<String, byte[]> template;
		private final ConsumerFactory<String, byte[]> factory;
		private final JdbcTemplate jdbc;

		public KafkaClientConfiguration(KafkaTemplate<String, byte[]> template,
				ConsumerFactory<String, byte[]> factory, DataSource datasSource) {
			this.jdbc = new JdbcTemplate(datasSource);
			this.template = template;
			this.factory = factory;
		}

		@PostConstruct
		public void init() {
			jdbc.update("UPDATE offsets SET offset=? WHERE id=1", 3);
			jdbc.update("DELETE FROM event WHERE offset >= ?", 3);
			try (Consumer<String, byte[]> consumer = factory.createConsumer()) {
				TopicPartition partition = new TopicPartition("input", 0);
				consumer.assign(Collections.singleton(partition));
				if (consumer.position(partition) > 2) {
					System.err.println("No need to seed logs");
					return;
				}
			}
			for (int i = 0; i < 5; i++) {
				this.template.send("input", ("foo" + i).getBytes());
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
