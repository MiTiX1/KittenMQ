package org.kittenmq.loadBalancers;

import org.kittenmq.brokers.Broker;
import org.kittenmq.consumers.Consumer;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;

import java.util.ArrayList;
import java.util.List;

public class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
    private final MessageQueue<Message<T>> queue;
    private final List<Consumer<T>> consumers = new ArrayList<>();
    private int currentIndex = 0;

    public RoundRobinLoadBalancer(MessageQueue<Message<T>> queue) {
        this.queue = queue;
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

    @Override
    public void registerConsumer(Consumer<T> consumer) {
        this.consumers.add(consumer);
    }
}
