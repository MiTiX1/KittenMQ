package org.kittenmq.queues;

import org.kittenmq.messages.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ConsumerMessageQueue<T> implements Queue<T> {
    private final BlockingQueue<Message<T>> queue = new LinkedBlockingQueue<>();

    @Override
    public void enqueue(Message<T> message) throws InterruptedException {
        this.queue.put(message);
    }

    @Override
    public Message<T> dequeue() throws InterruptedException {
        return this.queue.take();
    }

    public Message<T> dequeue(long timeout, TimeUnit unit) throws InterruptedException {
        return this.queue.poll(timeout, unit);
    }

    @Override
    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    @Override
    public String getName() {
        return "";
    }
}
