package com.example.demo;

import java.util.Collection;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.apache.kafka.common.TopicPartition;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.messaging.Message;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@Bean
	public Consumer<Message<?>> consumer() {
		return message -> {
			System.err.println(message);
		};
	}

	@Bean
	public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> listenerCustomizer(ConsumerConfiguration config) {
		return new ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>>() {

			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container,
					String destinationName, String group) {
				System.err.println("Customizing: " + destinationName);
				container.getContainerProperties().setConsumerRebalanceListener(
						new ConsumerAwareRebalanceListener() {
							@Override
							public void onPartitionsAssigned(
									org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
									Collection<TopicPartition> partitions) {
								for (TopicPartition partition : partitions) {
									System.err.println("Seeking: " + partition);
									consumer.seek(partition, config.getOffset());
								}
							}
						});
			}
		};
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
