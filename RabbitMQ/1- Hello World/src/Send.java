import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.util.Scanner;

public class Send {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://bechir:bechir@localhost:5672");
        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                Scanner sc = new Scanner(System.in)
        ) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println("type your message: ");
            String message = sc.nextLine();
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println("message sent `" + message + "`");

        }


    }


}
