package org.kittenmq.loadBalancers;

import org.kittenmq.consumers.Consumer;

import java.util.ArrayList;
import java.util.List;

public class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
    private final List<Consumer<T>> consumers = new ArrayList<>();
    private int currentIndex = 0;

    @Override
    public void registerConsumer(Consumer<T> consumer) {
        this.consumers.add(consumer);
    }

    @Override
    public Consumer<T> getNextConsumer() {
        if (this.consumers.isEmpty()) {
            return null;
        }
        Consumer<T> consumer = this.consumers.get(currentIndex);
        currentIndex = (currentIndex + 1) % this.consumers.size();
        return consumer;
    }
}
