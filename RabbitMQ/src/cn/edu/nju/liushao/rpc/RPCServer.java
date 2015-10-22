package cn.edu.nju.liushao.rpc;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class RPCServer {
	
	private static final String RPC_QUEUE_NAME = "rpc_queue";
	
	public static void main(String[] args) throws IOException, TimeoutException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		ConnectionFactory factory =  new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
		channel.basicQos(1);
		
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(RPC_QUEUE_NAME,false, consumer);
		System.out.println("server waiting for RPC requests");
		
		while(true){
			String response = null;
			
			Delivery delivery = consumer.nextDelivery();
			BasicProperties props = delivery.getProperties();
			BasicProperties replyProps = new BasicProperties.
											Builder()
											.correlationId(props.getCorrelationId()).build();
			String message = new String(delivery.getBody(),"utf-8");
			int n = Integer.parseInt(message);
			System.out.println(" [.] fib(" + message + ")");
				
			response = "" + fib(n);
			
			channel.basicPublish("", props.getReplyTo(), replyProps, response.getBytes("utf-8"));
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			
		}
	}
	
	private static int fib(int n){
		if(n == 0 || n == 1){
			return n;
		}
		return fib(n - 1) + fib(n - 2);
	}

}
