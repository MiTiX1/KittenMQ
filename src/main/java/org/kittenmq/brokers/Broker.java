package org.kittenmq.brokers;

import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;

import java.util.HashMap;
import java.util.Map;

public class Broker<T> {
    private final Map<String, MessageQueue<Message<T>>> queues = new HashMap<>();
    private final MessageQueue<Message<T>> deadLetterQueue;
    private final String messageStorePath;

    public Broker() {
        this.messageStorePath = "message-store";
        this.deadLetterQueue = new MessageQueue<>("deadLetterQueue", null, this.messageStorePath);
    }

    public Broker(String messageStorePath) {
        this.messageStorePath = messageStorePath;
        this.deadLetterQueue = new MessageQueue<>("deadLetterQueue", null, this.messageStorePath);
    }

    public void createQueue(String queueName) {
        if (!queues.containsKey(queueName)) {
            this.queues.put(queueName, new MessageQueue<>(queueName, this.deadLetterQueue, this.messageStorePath));
        }
    }

    public MessageQueue<Message<T>> getQueue(String queueName) {
        return queues.get(queueName);
    }

    public MessageQueue<Message<T>> getDeadLetterQueue() {
        return this.deadLetterQueue;
    }
}
