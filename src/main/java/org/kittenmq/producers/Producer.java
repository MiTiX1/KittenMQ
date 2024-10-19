package org.kittenmq.producers;

import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;

public class Producer<T> {
    private final MessageQueue<Message<T>> queue;

    public Producer(MessageQueue<Message<T>> queue) {
        this.queue = queue;
    }

    public void sendMessage(T payload) throws InterruptedException {
        Message<T> message = new Message<T>(payload);
        this.queue.enqueue(message);
    }
}
