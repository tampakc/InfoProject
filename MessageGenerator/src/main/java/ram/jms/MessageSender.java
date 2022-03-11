package ram.jms;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/*
 * This class is used to send a text message to the queue.
 */
public class MessageSender
{
	private static final long messageInterval = 1000 * 60 * 15; // Time between messages = 15 minutes
	private static final long windowTime = TimeUnit.HOURS.toMillis(6); //6 hours time window
	private static final int delayProbability = 30; // 1/30 probability of delay

	/*
	 * URL of the JMS server. DEFAULT_BROKER_URL will just mean that JMS server is on localhost
	 * 
	 * default broker URL is : tcp://localhost:61616"
	 */
	private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;

	/*
	 * Queue Name.You can create any/many queue names as per your requirement.
	 */
	private static String queueName = "Random_numbers";

    // Schedule the two timers to run with different delays.
    

	public static void main(String[] args) throws JMSException, InterruptedException
	{
		System.out.println("url = " + url);

		/*
		 * Getting JMS connection from the JMS server and starting it
		 */
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		/*
		 * Creating a non transactional session to send/receive JMS message.
		 */
		final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		/*
		 * The queue will be created automatically on the server.
		 */
		Destination destination = session.createQueue(queueName);
		
		// create instance of Random class
        Random rand = new Random();
        
		while (true) {		
			
			//The random number we wish to send
			final int rand_int = rand.nextInt(100);
			
			//The probability that 1 in 30 is late event
			int probability = rand.nextInt(delayProbability)+1; //probability 1 in 30
			System.out.println("Probability is " + probability);
			
			//The timestamp of when the message is made
		    final Timestamp timestamp = new Timestamp(System.currentTimeMillis());  
		    
		    /*
		     * Destination represents here our queue 'MESSAGE_QUEUE' on the JMS server.
		     * 
		     * MessageProducer is used for sending messages to the queue.
		     */
		    final MessageProducer producer = session.createProducer(destination);
		    
		    final BytesMessage message = session.createBytesMessage();
			final String time = String.valueOf(System.currentTimeMillis());
			final String val = String.valueOf(rand_int);
			String msg = val + " " + time;
			message.writeBytes(msg.getBytes());
		    
		    //The message consists of (num, timestamp) where num is the random number and timestamp is the timestamp of when the message is created
//		    final TextMessage message = session.createTextMessage(String.valueOf(rand_int)+" "+string_timestamp);
		    
		    //If it has to be a late event (1 in 30)
		    if (probability == 1) {
		    	System.out.println("I will print a delayed event in " + timestamp + " + 6 hours");
		    	Timer t = new Timer();  
		    	
		    	//Compute the time it will be sent (here 6 hours)
		    	final Timestamp timestamp2 =new Timestamp(System.currentTimeMillis());
				timestamp2.setTime(timestamp2.getTime() + windowTime);
				
		    	TimerTask tt = new TimerTask() {  
		    	    @Override  
		    	    public void run() {  
		    	    	try {
							producer.send(message);
			    	    	System.out.println("Delayed Number : " + val + " " + time + ", Timestamp of sent: " + timestamp2);
						} catch (JMSException e) {
							e.printStackTrace();
						} 
		    	    };  
		    	};  
				t.schedule(tt, timestamp2);
			    Thread.currentThread();

				// Suspend execution for 15 minutes
			    Thread.sleep(messageInterval); //15 mins
		    }
		    else {
		    System.out.println("Number : " + val + " " + time + ", Timestamp of sent: " + timestamp);
		    	
		    /*
		     * Here we are sending our message!
		     */
		    producer.send(message);

		    //Thread.currentThread();

			// Suspend execution for 15 minutes
		    Thread.sleep(messageInterval); //15 mins
		    }
		}
//		connection.close();
	}
}
