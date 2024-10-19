package org.kittenmq;

public class Producer<T> {
    private final MessageQueue<T> queue;

    public Producer(MessageQueue<T> queue) {
        this.queue = queue;
    }

    public void sendMessage(T message) throws InterruptedException {
        this.queue.enqueue(message);
    }
}
