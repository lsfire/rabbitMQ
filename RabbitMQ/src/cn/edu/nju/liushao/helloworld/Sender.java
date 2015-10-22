package cn.edu.nju.liushao.helloworld;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
public class Sender {

	private final static String QUEUE_NAME = "hello";

	  public static void main(String[] argv) throws Exception {
	    //create a connection to a server 
//		  The connection abstracts the socket connection, and takes care of protocol version negotiation and 
//		  authentication and so on for us. Here we connect to a broker on the local machine - hence the locallhost. 
//		  If we wanted to connect to a broker on a different machine we'd simply specify its name or IP address here.
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    Connection connection = factory.newConnection();
//	    Next we create a channel, which is where most of the API for getting things done resides.
	    Channel channel = connection.createChannel();

//	    To send, we must declare a queue for us to send to; then we can publish a message to the queue:
    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    String message = "Hello World!";
	    //publish the message to the queue
	    channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
	    System.out.println(" [x] Sent '" + message + "'");

	    channel.close();
	    connection.close();
	  }

}
