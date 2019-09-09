package com.example.demo;

import javax.annotation.PostConstruct;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest({ "spring.datasource.url=jdbc:tc:mysql:///test",
		"spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver" })
@DirtiesContext
@ExtendWith(OutputCaptureExtension.class)
public class DemoApplicationTests {

	static {
		@SuppressWarnings("resource")
		KafkaContainer kafka = new KafkaContainer();
		kafka.start();
		System.setProperty("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
	}

	@Test
	public void contextLoads(CapturedOutput output) throws Exception {
		assertThat(output.getErr()).doesNotContain("kafka_offset=2");
		assertThat(output.getErr()).contains("kafka_offset=3");
	}

	@TestConfiguration
	public static class KafkaClientConfiguration {

		private final KafkaTemplate<String, String> template;

		public KafkaClientConfiguration(KafkaTemplate<String, String> template) {
			this.template = template;
		}

		@PostConstruct
		public void init() {
			for (int i = 0; i < 5; i++) {
				this.template.send("input", "foo" + i);
			}
		}
	}

}
