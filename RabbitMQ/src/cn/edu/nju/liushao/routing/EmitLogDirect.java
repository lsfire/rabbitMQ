package cn.edu.nju.liushao.routing;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogDirect {
	private static final String EXCHANGE_NAME = "direct_logs";
	private static final String MQ_ADDRESS = "localhost";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
		//severity as a binding-key or called routing-key
		String severity = "info";
		String message = "helloworld";
		
		channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes(Charset.defaultCharset()));
		System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
		
		channel.close();
		connection.close();
	}

}
