package cn.edu.nju.liushao.routing;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

public class ReceiveLogsDirect {
	
	private static final String EXCHANGE_NAME = "direct_logs";
	private static final String MQ_ADDRESS = "localhost";
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
		/*
		 * below is the first channel with four binding-keys
		 */
		String queueName1 = channel.queueDeclare().getQueue();		
		System.out.println("first queue :" + queueName1);
		String[] bindingKeys = {"error","debug","warning","info"};
		//this queue is binded to an exchange with four binding-keys
		for(String severity : bindingKeys){
			channel.queueBind(queueName1, EXCHANGE_NAME, severity);
		}
		Consumer consumer1 = new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, 
					BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");
		        System.out.println(" first queue consumer received'" + envelope.getRoutingKey() + "':'" + message + "'");
			}
		};
		channel.basicConsume(queueName1,true, consumer1);
		
		/*
		 * below is the second queue with just one binding-key
		 */
		String queueName2 = channel.queueDeclare().getQueue();
		System.out.println("second queue : " + queueName2);
		channel.queueBind(queueName2, EXCHANGE_NAME, bindingKeys[0]);
		Consumer consumer2  = new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");
		        System.out.println(" second queue consumer received'" + envelope.getRoutingKey() + "':'" + message + "'");
			}
		};
		channel.basicConsume(queueName2, true, consumer2);
		
		
	}
	

}
