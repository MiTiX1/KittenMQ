package org.kittenmq.loadBalancers;

import org.kittenmq.consumers.Consumer;
import org.kittenmq.messages.Message;

public interface LoadBalancer<T> {
    Consumer<T> getNextConsumer();
    void registerConsumer(Consumer<T> consumer);
}
