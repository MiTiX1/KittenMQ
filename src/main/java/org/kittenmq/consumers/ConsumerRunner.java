package org.kittenmq.consumers;

import org.kittenmq.messages.Message;

public class ConsumerRunner<T> {
    private final Consumer<T> consumer;

    public ConsumerRunner(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    public void run(ConsumerCallback<T> callback) {
        Thread thread = new Thread(() -> {
            try {
                consumer.consume((ConsumerCallback<Message<T>>) callback);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        thread.start();
    }
}
