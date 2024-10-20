package org.kittenmq.consumers;

import org.kittenmq.messages.Message;

import java.io.IOException;

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

    public void run(ConsumerCallback<Message<T>> callback, int maxRetries, int retryDelay) {
        Thread thread = new Thread(() -> {
            try {
                consumer.consume(callback, maxRetries, retryDelay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Consumer thread was interrupted.");
            }
        });
        thread.start();
    }

    public void run(ConsumerCallback<Message<T>> callback, long timeout) {
        Thread thread = new Thread(() -> {
            try {
                consumer.consume(callback, timeout);
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                System.err.println("Consumer thread was interrupted.");
            }
        });
        thread.start();
    }

    public void run(ConsumerCallback<Message<T>> callback, int maxRetries, int retryDelay, long timeout) {
        Thread thread = new Thread(() -> {
            try {
                consumer.consume(callback, maxRetries, retryDelay, timeout);
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                System.err.println("Consumer thread was interrupted.");
            }
        });
        thread.start();
    }
}
