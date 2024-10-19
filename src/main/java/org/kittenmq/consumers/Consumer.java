package org.kittenmq.consumers;


import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;

public class Consumer<T> {
    private final MessageQueue<Message<T>> queue;

    public Consumer(MessageQueue<Message<T>> queue) {
        this.queue = queue;
    }

    public void consume(ConsumerCallback<Message<T>> callback) throws InterruptedException {
        while (true) {
            Message<T> message = this.queue.dequeue();
            callback.process(message);
        }
    }
}
