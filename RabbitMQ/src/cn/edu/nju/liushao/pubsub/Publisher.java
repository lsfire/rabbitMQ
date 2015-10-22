package cn.edu.nju.liushao.pubsub;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Publisher {
	private static final String EXCHANGE_NAME = "logs";
	private static final String MQ_ADDRESS = "localhost";
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		//the producer actually doesn't know which queque the message send to,
		//it just throw the message to the exchange and the exchange decides how the
		//message distribute
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		
		String[] messages = {"a","b","c","d"};
		String message = getMessage(messages);
		
		channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("utf-8"));
		
		channel.close();
		connection.close();
		
	}
	
	private static String getMessage(String[] strings){
	    if (strings.length < 1)
	    	    return "info: Hello World!";
	    return joinStrings(strings, ".");
	  }

	  private static String joinStrings(String[] strings, String delimiter) {
	    int length = strings.length;
	    if (length == 0) return "";
	    StringBuilder words = new StringBuilder(strings[0]);
	    for (int i = 1; i < length; i++) {
	        words.append(delimiter).append(strings[i]);
	    }
	    return words.toString();
	  }

}
