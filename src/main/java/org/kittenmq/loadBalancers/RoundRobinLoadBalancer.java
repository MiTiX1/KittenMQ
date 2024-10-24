package org.kittenmq.loadBalancers;

import org.kittenmq.consumers.Consumer;
import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
    private final List<Consumer<T>> consumers;
    private final MessageQueue<T> queue;
    private int currentIndex = 0;

    public RoundRobinLoadBalancer(MessageQueue<T> queue) {
        this(queue, new ArrayList<>());
    }

    public RoundRobinLoadBalancer(MessageQueue<T> queue, List<Consumer<T>> consumers) {
        this.queue = queue;
        this.consumers = consumers;
    }

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

    @Override
    public Message<T> getNextMessage() throws InterruptedException {
        return this.queue.dequeue(1000, TimeUnit.MILLISECONDS);
    }

    public MessageQueue<T> getQueue() {
        return this.queue;
    }

    public boolean areConsumersRunning() {
        for (Consumer<T> consumer : this.consumers) {
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
                    Message<T> message = this.getNextMessage();
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
