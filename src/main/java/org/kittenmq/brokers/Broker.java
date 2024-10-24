package org.kittenmq.brokers;

import org.kittenmq.consumers.Consumer;
import org.kittenmq.consumers.ConsumerRunner;
import org.kittenmq.loadBalancers.LoadBalancer;
import org.kittenmq.loadBalancers.RoundRobinLoadBalancer;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.DeadLetterQueue;
import org.kittenmq.queues.MessageQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Broker<T> {
    private final Map<String, MessageQueue<T>> queues = new HashMap<>();
    private final DeadLetterQueue<T> deadLetterQueue;
    private final Map<String, List<Consumer<T>>> consumers = new HashMap<>();
    private final Map<String, LoadBalancer<T>> loadBalancers = new HashMap<>();
    private final String messageStorePath;

    public Broker(String messageStorePath) {
        this.messageStorePath = messageStorePath;
        this.deadLetterQueue = new DeadLetterQueue<>("dead-letter-queue", this.getMessageStorePath());
    }

    public Broker() {
        this("message-store");
    }

    public void registerQueue(MessageQueue<T> queue) {
        if (!queues.containsKey(queue.getName())) {
            this.queues.put(queue.getName(), queue);
        }
    }

    public void registerConsumer(Consumer<T> consumer) {
        if (!consumers.containsKey(consumer.getQueueName())) {
            this.consumers.put(consumer.getQueueName(), new ArrayList<>());
        }
        this.consumers.get(consumer.getQueueName()).add(consumer);
    }

    public void registerLoadBalancer(RoundRobinLoadBalancer<T> loadBalancer) {
        if (!loadBalancers.containsKey(loadBalancer.getQueue().getName())) {
            this.loadBalancers.put(loadBalancer.getQueue().getName(), loadBalancer);
        }
    }

    public void run() {
        for (String queueName : this.queues.keySet() ) {
            for (Consumer<T> consumer : this.consumers.get(queueName)) {
                new ConsumerRunner<>(consumer).run();
            }
            this.loadBalancers.get(queueName).run();
        }
    }

    public MessageQueue<T> getQueue(String queueName) {
        return this.queues.get(queueName);
    }

    public DeadLetterQueue<T> getDeadLetterQueue() {
        return this.deadLetterQueue;
    }

    public String getMessageStorePath() {
        return this.messageStorePath;
    }
}
