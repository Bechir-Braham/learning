import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.util.Scanner;

public class NewTask {
    private final static String QUEUE_NAME = "hello_durable";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://bechir:bechir@localhost:5672");
        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                Scanner sc = new Scanner(System.in)
        ) {
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            System.out.println("type your message: ");
            String message = sc.nextLine();
            channel.basicPublish(
                    "", QUEUE_NAME,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes());
            System.out.println("message sent `" + message + "`");

        }


    }


}
