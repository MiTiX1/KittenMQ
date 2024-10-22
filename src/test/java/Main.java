import org.kittenmq.brokers.Broker;
import org.kittenmq.consumers.Consumer;
import org.kittenmq.messages.Message;
import org.kittenmq.producers.Producer;
import org.kittenmq.queues.MessageQueue;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        Broker<TestMessage> broker = new Broker<>();
        MessageQueue<Message<TestMessage>> queue = new MessageQueue<>("queue1", broker.getDeadLetterQueue(), broker.getMessageStorePath());
        MessageQueue<Message<TestMessage>> queue2 = new MessageQueue<>("queue2", broker.getDeadLetterQueue(), broker.getMessageStorePath());
        Producer<TestMessage> producer1 = new Producer<>("producer1", broker, "queue1");
        Producer<TestMessage> producer2 = new Producer<>("producer2", broker, "queue2");
        Producer<TestMessage> producer3 = new Producer<>("producer3", broker, "queue1");
        Consumer<TestMessage> consumer1 = new Consumer<>("consumer1", broker, "queue1", Main::foo, 10000);
        Consumer<TestMessage> consumer2 = new Consumer<>("consumer2", broker, "queue1", Main::foo, 20000);
        Consumer<TestMessage> consumer3 = new Consumer<>("consumer3", broker, "queue2", Main::foo, 20000);

        broker.registerQueue(queue);
        broker.registerQueue(queue2);
        broker.registerConsumer(consumer1);
        broker.registerConsumer(consumer2);
        broker.registerConsumer(consumer3);
        broker.run();

        producer3.sendMessage(new TestMessage("message: 20" , "john", "bob"));
        for (int i = 0; i < 5; i++) {
            producer1.sendMessage(new TestMessage("message: " + i, "john", "bob"));
        }
        producer2.sendMessage(new TestMessage("message: 10", "john", "bob"));
    }

    public static void foo(Message<TestMessage> message) {
        System.out.println("message: " + message.getPayload());
    }
}
