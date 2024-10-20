package org.kittenmq.messages;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageQueue<T> {
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<T>();;
    private final MessageQueue<T> deadLetterQueue;
    private final String name;

    public MessageQueue(String name, MessageQueue<T> deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
        this.name = name;
    }

    public void enqueue(T message) throws InterruptedException {
        this.queue.put(message);
    }

    public T dequeue() throws InterruptedException {
        return this.queue.take();
    }

    public T dequeue(long timeout, TimeUnit unit) throws InterruptedException {
        return this.queue.poll(timeout, unit);
    }

    public void moveToDeadLetterQueue(T message) throws InterruptedException {
        if (this.deadLetterQueue != null) {
            this.deadLetterQueue.enqueue(message);
        }
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public String getName() {
        return this.name;
    }
}
