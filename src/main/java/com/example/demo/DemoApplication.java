package com.example.demo;

import java.util.function.Consumer;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}
}

@ConditionalOnClass(EnableBinding.class)
@ConditionalOnProperty(prefix = "spring.cloud.stream", name = "enabled", havingValue = "true", matchIfMissing = true)
@Configuration
class StreamConfiguration {

	@Bean
	public Consumer<Message<?>> consumer() {
		return message -> {
			System.err.println(message);
		};
	}
}