import com.rabbitmq.client.*;
import com.rabbitmq.client.Consumer;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*

    Main idea about this snippet of code is show how to connect to RabbitMq and test yours grant permission to put and 
    pull messages to one queue
    
 */
public class ProducerAndConsumer {
    
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

    // Exchange's name that will be used to connect to RabbitMQ Server, default value is empty
    static final String RABBITMQ_EXCHANGE_NAME = "";

    // Flag that will be used to connect to RabbitMQ Server using SSL, default value is true
    static final boolean RABBITMQ_ENABLE_SSL = true;

    // Queue's name that will be used to send a message into RabbitMQ Server
    static final String RABBITMQ_QUEUE_NAME = "";

    // Queue's name that will be used to receive a message from RabbitMQ Server
    static final String RABBITMQ_REPLY_QUEUE_NAME = "";

    // Json Message that send to queue
    static String jsonMessage = "";
    
    public static void main(String args[]){

        try {

            // Create some identification to message 
            String id = "some_id";
            
            // Create the specific action to identify the message's content
            String action = "create";
            
            // Method to send a message
            sendMessage(id, action, jsonMessage);
            
            // Method to read all messages on reply queue
            getAllMessages();
            
        } catch ( Exception e){
            e.printStackTrace();
        }

    }
    
    private static void sendMessage(String id, String action, String jsonMessage) throws Exception {
        AMQP.BasicProperties amqpProperties = getAmqpProperties(id, action);
        Connection connection = openConnection();
        Channel channel = connection.createChannel();
        channel.basicPublish(RABBITMQ_EXCHANGE_NAME, RABBITMQ_QUEUE_NAME, amqpProperties, jsonMessage.getBytes());
        System.out.println("\n[x] Sent\n'" + jsonMessage + "'");
        channel.close();
        close(connection);
    }

    private static void getAllMessages() throws Exception {
        Channel channel = openConnection().createChannel();
        System.out.println("\n--------------------------------------------------------------------------------------");
        System.out.println("\n[*] Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                properties.getHeaders().forEach((k, v) -> System.out.println((k + ":" + v)));
                String message = new String(body, "UTF-8");
                System.out.println("\n[x] Received\n'" + message + "'");
            }
        };
        channel.basicConsume(RABBITMQ_REPLY_QUEUE_NAME, true, consumer);
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

    private static AMQP.BasicProperties getAmqpProperties(String id, String action) throws Exception{
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("correlationId", id);
        headers.put("action", action);
        return new AMQP.BasicProperties.Builder()
                .messageId(id)
                .deliveryMode(2)
                .contentType("application/json")
                .contentEncoding("utf-8")
                .replyTo(RABBITMQ_REPLY_QUEUE_NAME)
                .headers(headers)
                .timestamp(new Date())
                .correlationId(id)
                .build();
    }

    private static void close(Connection connection) {
        try{
            connection.close();
        } catch (Exception e) {}
    }

}
