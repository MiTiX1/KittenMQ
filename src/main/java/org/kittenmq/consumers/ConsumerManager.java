package org.kittenmq.consumers;

import java.util.ArrayList;
import java.util.List;

public class ConsumerManager<T> {
    private final List<Consumer<T>> consumers = new ArrayList<>();
    private final String queueName;

    public ConsumerManager(String queueName) {
        this.queueName = queueName;
    }

    public void registerConsumer(Consumer<T> consumer) {
        consumers.add(consumer);
    }

    public void unregisterConsumer(Consumer<T> consumer) {
        consumers.remove(consumer);
    }

    public void registerComsumers(List<Consumer<T>> consumers) {
        this.consumers.addAll(consumers);
    }

    public void unregisterComsumers(List<Consumer<T>> consumers) {
        this.consumers.removeAll(consumers);
    }

    public String getQueueName() {
        return this.queueName;
    }

    public List<Consumer<T>> getConsumers() {
        return this.consumers;
    }
}
