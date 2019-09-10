package com.example.demo;

import java.util.function.Consumer;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;

@SpringBootApplication
public class DemoApplication {

	private AcknowledgmentOffset ackoff;

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@Bean
	public Consumer<Message<?>> consumer() {
		return message -> {
			System.err.println(message);
			Long offset = message.getHeaders().get("kafka_offset", Long.class);
			if (offset != null) {
				Acknowledgment ack;
				if (ackoff != null && ackoff.offset < offset) {
					if (ackoff.offset < 2) {
						ack = ackoff.ack;
						System.err.println("Acking: " + offset);
						ack.acknowledge();
					}
				}
				ack = message.getHeaders().get("kafka_acknowledgment",
						Acknowledgment.class);
				if (ack != null) {
					System.err.println("Saving: " + offset + ", previous: " + ackoff);
					ackoff = new AcknowledgmentOffset(ack, offset);
				}
			}
		};
	}

	static class AcknowledgmentOffset {
		private Acknowledgment ack;
		long offset;

		AcknowledgmentOffset(Acknowledgment ack, long offset) {
			this.ack = ack;
			this.offset = offset;
		}

		@Override
		public String toString() {
			return "" + offset;
		}
	}
}