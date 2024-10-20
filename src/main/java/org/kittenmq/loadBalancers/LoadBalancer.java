package org.kittenmq.loadBalancers;

import org.kittenmq.consumers.Consumer;

public interface LoadBalancer<T> {
    void registerConsumer(Consumer<T> consumer);

    Consumer<T> getNextConsumer();
}
