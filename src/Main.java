import com.rabbitmq.client.*;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Main {
    
    static final String host = "";
    static final int    port = 5672;
    static final String username = "";
    static final String password = "";
    static final String queueName = "";
    static final String queueNameReplyTo = "";
    static final String exchangeName = "";
    static       String jsonMessage = "";
    
    public static void main(String args[]){

        try {

            String id = genId();
            jsonMessage = jsonMessage.replace("XXXXXXXX", id);
            sendMessage(id, jsonMessage);
            
            getAllMessages();
            
        } catch ( Exception e){
            e.printStackTrace();
        }

    }
    
    private static void sendMessage(String id, String jsonMessage) throws Exception {
        AMQP.BasicProperties amqpProperties = getAmqpProperties(id);
        Connection connection = open();
        Channel channel = connection.createChannel();
        channel.basicPublish(exchangeName, queueName, amqpProperties, jsonMessage.getBytes());
        System.out.println("\n[x] Sent\n'" + jsonMessage + "'");
        channel.close();
        close(connection);
    }

    private static void getAllMessages() throws Exception {
        Channel channel = open().createChannel();
        System.out.println("\n--------------------------------------------------------------------------------------");
        System.out.println("\n[*] Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("\n[x] Received\n'" + message + "'");
            }
        };
        channel.basicConsume(queueNameReplyTo, true, consumer);
    }
    
    
    private static String genId() throws Exception{ 
        final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        SecureRandom rnd = new SecureRandom();
        StringBuilder sb = new StringBuilder( 11 );
        sb.append("MQ_");
        for( int i = 0; i < 8; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
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

    private static AMQP.BasicProperties getAmqpProperties(String id) throws Exception{
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("correlationId", id);
        return new AMQP.BasicProperties.Builder()
                .messageId(id)
                .deliveryMode(2)
                .contentType("application/json")
                .contentEncoding("utf-8")
                .replyTo(queueNameReplyTo)
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
