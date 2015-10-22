package cn.edu.nju.liushao.worker;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

public class Worker {
	private static final String TASK_QUEUE_NAME = "task_queue";
	private static final String MQ_ADDRESS = "localhost";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(MQ_ADDRESS);
		final Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		
		channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
		System.out.println(" [*] waiting for messages. To exit press CTRL+C");
		
		channel.basicQos(1);
		
		final Consumer consumer  = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				// TODO Auto-generated method stub
				//super.handleDelivery(consumerTag, envelope, properties, body);
				String message = new String(body,"UTF-8");
				
				try {
					doWork(message);
				} finally {
					System.out.println("[x] done");
					// send back acknowledgement
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
				
				
			}
		};
		
		channel.basicConsume(TASK_QUEUE_NAME, false,consumer);
	}
	
	private static void doWork(String task){
		for(char c : task.toCharArray()){
			System.out.print(c + "\t");
			if(c == '.'){
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						Thread.currentThread().interrupt();
					}
				
			}
		}
	}
}
