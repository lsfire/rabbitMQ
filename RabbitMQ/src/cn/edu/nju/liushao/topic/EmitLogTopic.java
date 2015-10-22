package cn.edu.nju.liushao.topic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogTopic {
	private static final String EXCHANGE_NAME = "topic_logs";
	private static final String MQ_ADDRESS = "localhost";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		
		String routingKey = "quick.orange.lion";
		
		String message = "helloworld";
		channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("utf-8"));
		System.out.println(" producer sent: '" + routingKey + "':'" + message + "'");
	
	}

}
