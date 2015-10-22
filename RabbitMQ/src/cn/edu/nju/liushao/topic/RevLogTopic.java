package cn.edu.nju.liushao.topic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

public class RevLogTopic {
	private static final String EXCHANGE_NAME = "topic_logs";
	private static final String MQ_ADDRESS = "localhost";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		//following declare the first queue with three topic binding-keys
		String queueName1 = channel.queueDeclare().getQueue();
		System.out.println("queue1 = " + queueName1);
		String[] bindingKeys = {"*.orange.*","lazy.#","*.*.rabbit"};
		
		for(String bindingKey : bindingKeys){
			channel.queueBind(queueName1, EXCHANGE_NAME, bindingKey);
		}
		
		Consumer consumer1 = new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, 
					BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body,"utf-8");
				System.out.println("first queue consumer received " +envelope.getRoutingKey() + 
						"message : " + message);
			}
		};
		channel.basicConsume(queueName1,true, consumer1);
		//following is the second queue with just one topic binding-key
		String queueName2 = channel.queueDeclare().getQueue();
		System.out.println("queue2 = " + queueName2);
		
		channel.queueBind(queueName2, EXCHANGE_NAME, bindingKeys[1]);
		Consumer consumer2 = new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, 
					BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body,"utf-8");
				System.out.println("second queue receive " + "bindingKey: " + envelope.getRoutingKey() 
				+ " message: " + message);
			}
		};
		channel.basicConsume(queueName2,true, consumer2);
		
	}

}
