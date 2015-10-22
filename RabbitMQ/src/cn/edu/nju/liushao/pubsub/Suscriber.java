package cn.edu.nju.liushao.pubsub;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;

public class Suscriber {
	private static final String EXCHANGE_NAME = "logs";
	private static final String MQ_ADDRESS = "localhost";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		//get a temporary queue,
		String queueName = channel.queueDeclare().getQueue();
		System.out.println("the queue name is " + queueName);
		//bind the first queue to the exchange
		channel.queueBind(queueName, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		
		Consumer consumer = new DefaultConsumer(channel){
			public void handleDelivery(String consumerTag, com.rabbitmq.client.Envelope envelope, 
					com.rabbitmq.client.AMQP.BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body,"utf-8");
				System.out.println(" [x] Received '" + message + "'");
			}
		};
		channel.basicConsume(queueName,true, consumer);
		
		//the second queue
		String queueName1 = channel.queueDeclare().getQueue();
		System.out.println("the queue name1 is " + queueName1);
		//bind the second queue to the exchange,fanout just ignore the binding key
		channel.queueBind(queueName1, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		
		Consumer consumer1 = new DefaultConsumer(channel){
			public void handleDelivery(String consumerTag, com.rabbitmq.client.Envelope envelope, 
					com.rabbitmq.client.AMQP.BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body,"utf-8");
				System.out.println(" [x1] Received '" + message + "'");
			}
		};
		channel.basicConsume(queueName1,true, consumer1);
	}

}
