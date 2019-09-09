package com.example.demo;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
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
	ConsumerConfiguration config(DataSource datasSource) {
		return new ConsumerConfiguration(datasSource);
	}

}

class ConsumerConfiguration {
	private int partition = 0;
	private long offset = 0;
	private String topic = "input";
	private JdbcTemplate template;
	private boolean initialized = false;

	public ConsumerConfiguration(DataSource datasSource) {
		this.template = new JdbcTemplate(datasSource);
	}

	public int getPartition() {
		init();
		return this.partition;
	}

	public long getOffset() {
		init();
		return this.offset;
	}

	public String getTopic() {
		init();
		return this.topic;
	}

	private void init() {
		if (this.initialized) {
			return;
		}
		this.offset = this.template.queryForObject("SELECT offset FROM offsets where id=1", Long.class);
		this.initialized = true;
	}

}
