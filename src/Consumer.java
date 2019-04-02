import com.rabbitmq.client.*;

import java.io.IOException;

public class Consumer {

    static final String host = "";
    static final int    port = 5672;
    static final String username = "";
    static final String password = "";
    static final String queueName = "";

    public static void main(String args[]){

        try {

            getAllMessages();

        } catch ( Exception e){
            e.printStackTrace();
        }

    }

    private static void getAllMessages() throws Exception {
        Channel channel = open().createChannel();
        System.out.println("\n--------------------------------------------------------------------------------------");
        System.out.println("\n[*] Waiting for messages. To exit press CTRL+C");
        com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("\n[x] Received\n'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    private static Connection open() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.useSslProtocol();
        return factory.newConnection();
    }
}
