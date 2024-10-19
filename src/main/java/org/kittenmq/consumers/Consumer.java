package org.kittenmq.consumers;

import org.kittenmq.MessageQueue;

public class Consumer<T> {
    private final MessageQueue<T> queue;

    public Consumer(MessageQueue<T> queue) {
        this.queue = queue;
    }

    public void consume(ConsumerCallback<T> callback) throws InterruptedException {
        while (true) {
            T message = this.queue.dequeue();
            callback.process(message);
        }
    }
}
