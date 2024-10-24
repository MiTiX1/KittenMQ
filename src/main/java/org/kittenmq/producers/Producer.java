package org.kittenmq.producers;

import org.kittenmq.brokers.Broker;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;

import java.io.IOException;
import java.util.Map;

public class Producer<T> {
    private final String name;
    private final Broker<T> broker;
    private final String queueName;

    public Producer(String name, Broker<T> broker, String queueName) {
        this.name = name;
        this.broker = broker;
        this.queueName = queueName;
    }

    public void sendMessage(T payload) throws InterruptedException, IOException {
        MessageQueue<T> queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }
        Message<T> message = new Message<>(payload);
        queue.enqueue(message);
    }

    public void sendMessage(T payload, Map<String, String> headers) throws InterruptedException, IOException {
        MessageQueue<T> queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }
        Message<T> message = new Message<T>(payload, headers);
        queue.enqueue(message);
    }

    public String getName() {
        return this.name;
    }
}
