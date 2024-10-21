import org.kittenmq.brokers.Broker;
import org.kittenmq.consumers.Consumer;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;
import org.kittenmq.producers.Producer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        String queueId = "queue1";

        Broker<TestMessage> broker = new Broker<>();
        MessageQueue<Message<TestMessage>> queue = new MessageQueue<>(queueId, broker.getDeadLetterQueue(), broker.getMessageStorePath());
        Producer<TestMessage> producer = new Producer<>("producer1", broker, queueId);
        Consumer<TestMessage> consumer1 = new Consumer<>("consumer1", broker, queueId, Main::foo, 10000);
        Consumer<TestMessage> consumer2 = new Consumer<>("consumer2", broker, queueId, Main::foo, 10000);

        broker.registerQueue(queue);
        broker.registerProducer(producer);
        broker.registerConsumer(consumer1);
        broker.registerConsumer(consumer2);
        broker.run();

        TestMessage testMessage1 = new TestMessage("message sent by john", "john", "bob");
        TestMessage testMessage2 = new TestMessage("message sent by bob", "bob", "john");

        for (int i = 0; i < 10; i++) {
            producer.sendMessage(new TestMessage("message " + i, "john", "bob"));
        }
    }

    public static void foo(Message<TestMessage> message) {
        System.out.println("message: " + message.getPayload());
    }
}
