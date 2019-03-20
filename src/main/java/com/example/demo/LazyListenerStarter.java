/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.demo;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class LazyListenerStarter
		implements SmartInitializingSingleton, ApplicationContextAware {

	private final CachingConnectionFactory cf;

	private final String queue;

	private final String beanName;

	private ApplicationContext applicationContext;

	public LazyListenerStarter(CachingConnectionFactory cf, String queue,
			String beanName) {
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
		System.out.println("Submitting start...");
		final ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.execute(() -> {
			try {
				System.out.println("Starting listener...");
				// use the underlying RabbitCF to avoid triggering admin provisioning
				Connection conn = this.cf.getRabbitConnectionFactory().newConnection();
				Channel channel = conn.createChannel();
				GetResponse got = channel.basicGet(this.queue, false);
				if (got != null) {
					startIt(conn, channel, got.getEnvelope().getDeliveryTag());
					exec.shutdown();
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
								exec.shutdown();
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
