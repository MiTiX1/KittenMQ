package org.kittenmq.loadBalancers;

import org.kittenmq.consumers.Consumer;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;

public interface LoadBalancer<T> {
    void registerConsumer(Consumer<T> consumer);
    void run();
    Consumer<T> getNextConsumer();
    Message<T> getNextMessage() throws InterruptedException;
    MessageQueue<T> getQueue();
}
