package org.kittenmq.producers;

import org.kittenmq.brokers.Broker;
import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;

public class Producer<T> {
    private final Broker broker;
    private final String queueName;

    public Producer(Broker broker, String queueName) {
        this.broker = broker;
        this.queueName = queueName;
    }

    public void sendMessage(T payload) throws InterruptedException {
        MessageQueue<Message<?>> queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }
        Message<T> message = new Message<T>(payload);
        queue.enqueue(message);
    }
}
