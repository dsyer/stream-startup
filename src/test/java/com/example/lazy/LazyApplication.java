package com.example.lazy;

import com.example.demo.LazyListenerStarter;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class LazyApplication {

	public static void main(String[] args) {
		SpringApplication.run(LazyApplication.class, args);
	}

	@Bean
	public LazyListenerStarter starter(CachingConnectionFactory cf) {
		System.out.println("Getting starter");
		return new LazyListenerStarter(cf, "foo", "listener");
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

@Component
@Lazy
class Listener {

	@RabbitListener(queues = "foo")
	public void listen(String in) {
		System.out.println(in);
	}

}
