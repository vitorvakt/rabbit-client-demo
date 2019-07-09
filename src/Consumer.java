import com.rabbitmq.client.*;

import java.io.IOException;

public class Consumer {

    // Host that will be used to connect to RabbitMQ Server
    // Do not need to use protocols like amqp:// or https:// 
    static final String RABBITMQ_HOST_SERVER = "";

    // Port that will be used to connect to RabbitMQ Server
    static final int RABBITMQ_HOST_PORT = 5672;

    // User that will be used to connect to RabbitMQ Server
    static final String RABBITMQ_USERNAME = "";

    // Password that will be used to connect to RabbitMQ Server
    static final String RABBITMQ_PASSWORD = "";

    // Virtual host's name that will be used to connect to RabbitMQ Server, default value is empty
    static final String RABBITMQ_VIRTUAL_HOST_NAME = "";

    // Flag that will be used to connect to RabbitMQ Server using SSL, default value is true
    static final boolean RABBITMQ_ENABLE_SSL = true;

    // Queue's name that will be used to read a message from RabbitMQ Server
    static final String RABBITMQ_QUEUE_NAME = "";

    // Json Message that send to queue
    static String jsonMessage = "";

    public static void main(String args[]){

        try {
            // Method to read all messages on reply queue
            getAllMessages();

        } catch ( Exception e){
            e.printStackTrace();
        }

    }

    private static void getAllMessages() throws Exception {
        Channel channel = openConnection().createChannel();
        System.out.println("\n--------------------------------------------------------------------------------------");
        System.out.println("\n[*] Waiting for messages. To exit press CTRL+C");
        com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                properties.getHeaders().forEach((k, v) -> System.out.println((k + ":" + v)));
                String message = new String(body, "UTF-8");
                System.out.println("\n[x] Received\n'" + message + "'");
            }
        };
        channel.basicConsume(RABBITMQ_QUEUE_NAME, true, consumer);
    }

    private static Connection openConnection() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST_SERVER);
        factory.setPort(RABBITMQ_HOST_PORT);
        factory.setUsername(RABBITMQ_USERNAME);
        factory.setPassword(RABBITMQ_PASSWORD);
        if(!RABBITMQ_VIRTUAL_HOST_NAME.trim().equals("")) {
            factory.setVirtualHost(RABBITMQ_VIRTUAL_HOST_NAME);
        }
        if(RABBITMQ_ENABLE_SSL) {
            factory.useSslProtocol();
        }
        return factory.newConnection();
    }
}
