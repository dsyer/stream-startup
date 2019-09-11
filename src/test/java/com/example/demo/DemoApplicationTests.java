package com.example.demo;

import java.util.Collections;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
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

	@Test
	public void contextLoads(CapturedOutput output) throws Exception {
		String err = Awaitility.await().until(output::getErr,
				value -> value.contains("kafka_offset=3"));
		assertThat(err).doesNotContain("kafka_offset=2");
	}

	@TestConfiguration
	public static class KafkaClientConfiguration {

		private final KafkaTemplate<String, byte[]> template;
		private final ConsumerFactory<String, byte[]> factory;

		public KafkaClientConfiguration(KafkaTemplate<String, byte[]> template,
				ConsumerFactory<String, byte[]> factory) {
			this.template = template;
			this.factory = factory;
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
