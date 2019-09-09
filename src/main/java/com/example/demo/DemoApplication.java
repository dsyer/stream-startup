package com.example.demo;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;

@SpringBootApplication(proxyBeanMethods = false)
public class DemoApplication {

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@KafkaListener(topicPartitions = { @TopicPartition(partitionOffsets = {
			@PartitionOffset(partition = "#{config.partition}", initialOffset = "#{config.offset}") }, topic = "#{config.topic}") })
	public void consumer(Message<?> message) {
		System.err.println(message);
		Long offset = message.getHeaders().get("kafka_offset", Long.class);
		if (offset != null && offset < 2) {
			Acknowledgment ack = message.getHeaders().get("kafka_acknowledgment",
					Acknowledgment.class);
			if (ack != null) {
				System.err.println("Acking: " + offset);
				ack.acknowledge();
			}
		}
	}

	@Bean
	ConsumerConfiguration config() {
		return new ConsumerConfiguration();
	}

}

@ConfigurationProperties("kafka")
class ConsumerConfiguration {
	private int partition;
	private long offset;
	private String topic;

	public int getPartition() {
		return this.partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return this.offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
