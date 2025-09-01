
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";
    private static final Scanner sc = new Scanner(System.in);

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

                String severity = getString("severity");
                String message = getString("message");
                channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }

    private static String getString(String out){
        System.out.println("enter the "+out+":");
        return sc.nextLine();
    }

}
