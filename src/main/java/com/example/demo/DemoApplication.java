package com.example.demo;

import java.util.Date;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.handler.annotation.Payload;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws Exception {
		new SpringApplicationBuilder(DemoApplication.class).run(args);
	}

	@Bean
	public CommandLineRunner runner() {
		return args -> {
			Thread t = new Thread(() -> {
				while (true) {
					try {
						Thread.sleep(100L);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			});
			t.setDaemon(false);
			t.start();
		};
	}

}

@ConditionalOnClass(EnableBinding.class)
@ConditionalOnProperty(prefix = "spring.cloud.stream", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableBinding(Sink.class)
class StreamConfiguration {

}

@ConditionalOnClass(Queue.class)
@ConditionalOnMissingClass("org.springframework.cloud.stream.annotation.EnableBinding")
@Configuration
class RabbitConfiguration {

	@Bean
	public Queue fooQueue() {
		return new Queue("foo");
	}

	@Bean
	@Lazy
	public RabbitConsumer rabbitListener() {
		return new RabbitConsumer();
	}

	@Bean
	public LazyListenerStarter starter(CachingConnectionFactory cf) {
		return new LazyListenerStarter(cf, "foo", "rabbitListener");
	}

}

@RabbitListener(queues = "foo")
class RabbitConsumer {

	@RabbitHandler
	public void process(@Payload String foo) {
		System.out.println(new Date() + ": " + foo);
	}

}
