import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsTopic {

    private static final String RABBIT_MQ_HOST = "localhost";
    private static final String EXCHANGE_NAME = "topic_skilift";
    private static final String EXCHANGE_TYPE = "topic";
    private static final String ROUTING_KEY = "skilift.#";
    private static final int RECONNECT_INTERVAL = 5000;

    public static void main(String[] argv) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_MQ_HOST);

        while (true) {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);

                System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Received '" +
                            delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
                };

                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

                // Block this thread to keep the consumer alive
                synchronized (connection) {
                    connection.wait();
                }

            } catch (Exception e) {
                System.err.println("Connection failed. Retrying in 5 seconds...");
                try {
                    Thread.sleep(RECONNECT_INTERVAL);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
