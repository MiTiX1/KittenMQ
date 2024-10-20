package org.kittenmq.consumers;


import org.kittenmq.brokers.Broker;
import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;

public class Consumer<T> {
    private final Broker broker;
    private final String queueName;

    public Consumer(Broker broker, String queueName) {
        this.broker = broker;
        this.queueName = queueName;
    }

    public void consume(ConsumerCallback<Message<T>> callback) throws InterruptedException {
        MessageQueue<Message<?>> queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }

        while (true) {
            Message<T> message = (Message<T>) queue.dequeue();
            callback.process(message);
        }
    }
}
