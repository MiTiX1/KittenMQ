package org.kittenmq.consumers;

public class ConsumerRunner<T> {
    private final Consumer<T> consumer;

    public ConsumerRunner(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    public void run(ConsumerCallback<T> callback) {
        Thread thread = new Thread(() -> {
            try {
                consumer.consume(callback);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        thread.start();
    }
}
