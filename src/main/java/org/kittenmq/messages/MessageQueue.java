package org.kittenmq.messages;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MessageQueue<T> {
    private final BlockingQueue<T> queue;
    private final String name;

    public MessageQueue(String name) {
        this.queue = new LinkedBlockingQueue<T>();
        this.name = name;
    }

    public MessageQueue(String name, int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.name = name;
    }

    public void enqueue(T message) throws InterruptedException {
        this.queue.put(message);
    }

    public T dequeue() throws InterruptedException {
        return this.queue.take();
    }

    public String getName() {
        return this.name;
    }
}
