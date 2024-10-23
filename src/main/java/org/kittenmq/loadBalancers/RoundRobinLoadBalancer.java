package org.kittenmq.loadBalancers;

import org.kittenmq.consumers.Consumer;
import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
    private final List<Consumer<?>> consumers;
    private final MessageQueue<T> queue;
    private int currentIndex = 0;

    public RoundRobinLoadBalancer(MessageQueue<T> queue) {
        this(queue, new ArrayList<>());
    }

    public RoundRobinLoadBalancer(MessageQueue<T> queue, List<Consumer<?>> consumers) {
        this.queue = queue;
        this.consumers = consumers;
    }

    @Override
    public <U> void registerConsumer(Consumer<U> consumer) {
        this.consumers.add(consumer);
    }

    @Override
    public Consumer<T> getNextConsumer() {
        if (this.consumers.isEmpty()) {
            return null;
        }
        Consumer<T> consumer = (Consumer<T>) this.consumers.get(currentIndex);
        currentIndex = (currentIndex + 1) % this.consumers.size();
        return consumer;
    }

    @Override
    public Message<T> getNextMessage() throws InterruptedException {
        return (Message<T>) this.queue.dequeue();
    }

    public MessageQueue<T> getQueue() {
        return (MessageQueue<T>) this.queue;
    }

    public boolean areConsumersRunning() {
        for (Consumer<?> consumer : this.consumers) {
            if (consumer.isRunning()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void run() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    if (!areConsumersRunning()) {
                        ErrorHandler.logWarning("No consumer running");
                        break;
                    }
                    Message<T> message = (Message<T>) this.queue.dequeue(1000, TimeUnit.MILLISECONDS);
                    Consumer<T> consumer = this.getNextConsumer();
                    if (consumer != null && message != null) {
                        consumer.getQueue().enqueue(message);
                    }
                } catch (InterruptedException e) {
                    ErrorHandler.logError("Error", e);
                    Thread.currentThread().interrupt();
                }
            }
        });
        thread.start();
    }
}
