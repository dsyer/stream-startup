package com.example.lazy;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.example.demo.LazyListenerStarter;

@SpringBootApplication
public class LazyApplication {

	public static void main(String[] args) {
		SpringApplication.run(LazyApplication.class, args);
	}

	@Bean
	public LazyListenerStarter starter(CachingConnectionFactory cf) {
		return new LazyListenerStarter(cf, "foo", "listener");
	}

}

@Component
@Lazy
class Listener {

	@RabbitListener(queues = "foo")
	public void listen(String in) {
		System.out.println(in);
	}

}
