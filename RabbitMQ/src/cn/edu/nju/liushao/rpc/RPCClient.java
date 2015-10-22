package cn.edu.nju.liushao.rpc;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class RPCClient {
	private Connection connection;
	private Channel channel;
	private String requestQueueName = "rpc_queue";
	private String replyQueueName ;
	private QueueingConsumer consumer;
	
	public RPCClient() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();
	
		replyQueueName = channel.queueDeclare().getQueue();
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(replyQueueName,true, consumer);
	}
	
	public String call(String message) throws UnsupportedEncodingException, IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		String response = null;
		String corrId = UUID.randomUUID().toString();
		System.out.println("the corrId is " + corrId);
		
		BasicProperties props = new BasicProperties.Builder().
								correlationId(corrId).replyTo(replyQueueName).
								build();
		channel.basicPublish("", requestQueueName, props, message.getBytes("utf-8"));
		while(true){
			Delivery delivery = consumer.nextDelivery();
			if(delivery.getProperties().getCorrelationId().equals(corrId)){
				response = new String(delivery.getBody(),"utf-8");
				break;
			}
			
		}
		
		return response;
		
	}
	
	public void close() throws IOException, TimeoutException{
		channel.close();
		connection.close();
	}
	
	public static void main(String[] args) {
		RPCClient client = null;
		String response = null;
		
		try {
			client = new RPCClient();
			response = client.call("15");
			System.out.println("client got the response :" + response);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				if(null != client){
					client.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	

}
