package org.kittenmq.consumers;

import org.kittenmq.messages.Message;

import java.io.IOException;

public class ConsumerRunner<T> {
    private final Consumer<T> consumer;

    public ConsumerRunner(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    public void run() {
        Thread thread = new Thread(() -> {
            try {
                consumer.consume();
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        thread.start();
    }
}
