package org.kittenmq.brokers;

import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;
import org.kittenmq.producers.Producer;

import java.util.HashMap;
import java.util.Map;

public class Broker {
    private final Map<String, MessageQueue<Message<?>>> queues = new HashMap<>();

    public void createQueue(String queueName) {
        if (!queues.containsKey(queueName)) {
            this.queues.put(queueName, new MessageQueue<>(queueName));
        }
    }

    public void createQueue(String queueName, int capacity) {
        if (!queues.containsKey(queueName)) {
            this.queues.put(queueName, new MessageQueue<>(queueName, capacity));
        }
    }

    public MessageQueue<Message<?>> getQueue(String queueName) {
        return queues.get(queueName);
    }
}
