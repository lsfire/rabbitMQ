package cn.edu.nju.liushao.worker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

	  private static final String TASK_QUEUE_NAME = "task_queue";
	  private static final String MQ_ADDRESS = "localhost";
	  public static void main(String[] argv) throws Exception {
		  /*
		   * init factory,connection and channel 
		   */
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(MQ_ADDRESS);
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();
	    //declare a queue
	    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

	    String[] messages = {"a","b","c","d"};
	    String message = getMessage(messages);
	    //the concept of channel in rabbitMQ,the first parameter defines the name of exchange,
	    //"" means the default exchange
	    channel.basicPublish("", TASK_QUEUE_NAME,
	        MessageProperties.PERSISTENT_TEXT_PLAIN,
	        message.getBytes("UTF-8"));
	    System.out.println(" [x] Sent '" + message + "'");

	    channel.close();
	    connection.close();
	  }

	  private static String getMessage(String[] strings) {
	    if (strings.length < 1)
	      return "Hello World!";
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
