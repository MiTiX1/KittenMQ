import org.kittenmq.brokers.Broker;
import org.kittenmq.connections.Connection;
import org.kittenmq.connections.ConnectionManager;
import org.kittenmq.consumers.Consumer;
import org.kittenmq.consumers.ConsumerRunner;
import org.kittenmq.messages.Message;
import org.kittenmq.producers.Producer;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        ConnectionManager connectionManager = new ConnectionManager();

        String queueId = "queue1";
        String producerId = "producer1";
        String consumerId = "consumer1";

        Connection producerConnection = connectionManager.createConnection(producerId, broker);
        Connection consumerConnection = connectionManager.createConnection(consumerId, broker);

        broker.createQueue(queueId);
        Producer<TestMessage> producer = new Producer<>(producerId, producerConnection.getBroker(), queueId);
        Consumer<Message<TestMessage>> consumer = new Consumer<Message<TestMessage>>(consumerId, consumerConnection.getBroker(), queueId);

        ConsumerRunner<Message<TestMessage>> consumerRunner = new ConsumerRunner<>(consumer);
        consumerRunner.run(message -> {
            System.out.println("Consumed" + message.getPayload());
        }, 10000);

        TestMessage testMessage1 = new TestMessage("message sent by john", "john", "bob");
        TestMessage testMessage2 = new TestMessage("message sent by bob", "bob", "john");

        producer.sendMessage(testMessage1);
        producer.sendMessage(testMessage2);

        connectionManager.closeConnection(producerId);
        connectionManager.closeConnection(consumerId);
    }
}
