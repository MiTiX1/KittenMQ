import org.kittenmq.brokers.Broker;
import org.kittenmq.consumers.Consumer;
import org.kittenmq.messages.Message;
import org.kittenmq.producers.Producer;
import org.kittenmq.queues.MessageQueue;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Initialize the broker
        Broker<TestMessage> broker = new Broker<>();

        // Create message queues
        MessageQueue<Message<TestMessage>> queue1 = new MessageQueue<>("queue1", broker.getDeadLetterQueue(), broker.getMessageStorePath());

        // Create producers
        Producer<TestMessage> producer1 = new Producer<>("producer1", broker, "queue1");

        // Create consumers and assign message handler callbacks
        Consumer<TestMessage> consumer1 = new Consumer<>("consumer1", broker, "queue1", Main::processMessage, 10000);
        Consumer<TestMessage> consumer2 = new Consumer<>("consumer2", broker, "queue1", Main::processMessage, 20000);

        // Register queues and consumers to the broker
        broker.registerQueue(queue1);
        broker.registerConsumer(consumer1);
        broker.registerConsumer(consumer2);

        // Start the broker service
        broker.run();

        // Send messages via producers
        for (int i = 0; i < 5; i++) {
            producer1.sendMessage(new TestMessage("message: " + i, "john", "bob"));
        }
    }

    // Message handler function
    public static void processMessage(Message<TestMessage> message) {
        System.out.println("message: " + message.getPayload());
    }
}
