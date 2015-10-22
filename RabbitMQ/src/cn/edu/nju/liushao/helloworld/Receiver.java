package cn.edu.nju.liushao.helloworld;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

public class Receiver {
	
	private static final String QUEUE_NAME = "hello";
	private static final String HOST_ADDRESS = "localhost"; 
	public static void main(String[] args) throws IOException, TimeoutException {
		//Setting up is the same as the sender; 
//		we open a connection and a channel, and declare the queue from which we're going to consume. 
//		Note this matches up with the queue that send publishes to.
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST_ADDRESS);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
//		Note that we declare the queue here, as well. 
//		Because we might start the receiver before the sender, 
//		we want to make sure the queue exists before we try to consume messages from it.
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println("[*] waiting for message .To exit press CTRL+C");
//		We're about to tell the server to deliver us the messages from the queue. 
//		Since it will push us messages asynchronously, we provide a callback in the form of an object that 
//		will buffer the messages until we're ready to use them. That is what a DefaultConsumer subclass does.
		Consumer consumer = new DefaultConsumer(channel){
			
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body,"UTF-8");
				System.out.println(" [x] Received '" + message + "'");
			}
		};
		channel.basicConsume(QUEUE_NAME,true, consumer);
		
		
		
	}

}
