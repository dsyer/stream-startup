package com.example.lazy;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class LazyApplication {

	public static void main(String[] args) {
		SpringApplication.run(LazyApplication.class, args);
	}

	@Bean
	public LazyListenerStarter starter(ConnectionFactory cf) {
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

class LazyListenerStarter implements SmartInitializingSingleton, ApplicationContextAware {

	private final ConnectionFactory cf;

	private final String queue;

	private final String beanName;

	private ApplicationContext applicationContext;

	LazyListenerStarter(ConnectionFactory cf, String queue, String beanName) {
		this.cf = cf;
		this.queue = queue;
		this.beanName = beanName;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void afterSingletonsInstantiated() {
		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				Connection conn = this.cf.createConnection();
				Channel channel = conn.createChannel(false);
				GetResponse got = channel.basicGet(this.queue, false);
				if (got != null) {
					startIt(conn, channel, got.getEnvelope().getDeliveryTag());
				}
				else {
					channel.basicConsume(this.queue, new DefaultConsumer(channel) {

						@Override
						public void handleDelivery(String consumerTag, Envelope envelope,
								BasicProperties properties, byte[] body)
								throws IOException {

							try {
								getChannel().basicCancel(getConsumerTag());
								startIt(conn, getChannel(), envelope.getDeliveryTag());
							}
							catch (TimeoutException e) {
								e.printStackTrace();
							}
						}
					});
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	private void startIt(Connection conn, Channel channel, long deliveryTag)
			throws IOException, TimeoutException {
		channel.basicReject(deliveryTag, true);
		channel.close();
		conn.close();
		this.applicationContext.getBean(this.beanName); // DEADLOCK here
		System.out.println("Started");
	}

}