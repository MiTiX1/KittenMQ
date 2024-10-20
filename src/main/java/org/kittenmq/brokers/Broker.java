package org.kittenmq.brokers;

import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;
import org.kittenmq.producers.Producer;

import java.util.HashMap;
import java.util.Map;

public class Broker {
    private final Map<String, MessageQueue<Message<?>>> queues = new HashMap<>();
    private final MessageQueue<Message<?>> deadLetterQueue;

    public Broker() {
        this.deadLetterQueue = new MessageQueue<>("deadLetterQueue", null);
    }

    public void createQueue(String queueName) {
        if (!queues.containsKey(queueName)) {
            this.queues.put(queueName, new MessageQueue<>(queueName, this.deadLetterQueue));
        }
    }

    public MessageQueue<Message<?>> getQueue(String queueName) {
        return queues.get(queueName);
    }

    public MessageQueue<Message<?>> getDeadLetterQueue() {
        return deadLetterQueue;
    }
}
