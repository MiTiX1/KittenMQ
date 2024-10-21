package org.kittenmq.brokers;

import org.kittenmq.consumers.Consumer;
import org.kittenmq.consumers.ConsumerManager;
import org.kittenmq.consumers.ConsumerRunner;
import org.kittenmq.loadBalancers.LoadBalancer;
import org.kittenmq.loadBalancers.RoundRobinLoadBalancer;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;
import org.kittenmq.producers.Producer;
import org.kittenmq.queues.DeadLetterQueue;

import java.util.HashMap;
import java.util.Map;

public class Broker<T> {
    private final Map<String, MessageQueue<Message<T>>> queues = new HashMap<>();
    private final Map<String, Producer<T>> producers = new HashMap<>();
    private final Map<String, ConsumerManager<T>> consumers = new HashMap<>();
    private final Map<String, ConsumerRunner<T>> runners = new HashMap<>();
    private final Map<String, LoadBalancer<T>> loadBalancers = new HashMap<>();
    private final DeadLetterQueue<Message<T>> deadLetterQueue;
    private final String messageStorePath;

    public Broker() {
        this("message-store");
    }

    public Broker(String messageStorePath) {
        this.messageStorePath = messageStorePath;
        this.deadLetterQueue = new DeadLetterQueue<>("dead-letter-queue", this.getMessageStorePath());
    }

    public void registerQueue(MessageQueue<Message<T>> queue) {
        if (!queues.containsKey(queue.getName())) {
            this.queues.put(queue.getName(), queue);
            this.loadBalancers.put(queue.getName(), new RoundRobinLoadBalancer<>(queue));
        }
    }

    public void registerProducer(Producer<T> producer) {
        if (!producers.containsKey(producer.getName())) {
            this.producers.put(producer.getName(), producer);
        }
    }

    public void registerConsumer(Consumer<T> consumer) {
        if (!consumers.containsKey(consumer.getName())) {
            this.consumers.put(consumer.getQueueName(), new ConsumerManager<>(consumer.getQueueName()));
        }
        this.consumers.get(consumer.getQueueName()).registerConsumer(consumer);
        this.loadBalancers.get(consumer.getQueueName()).registerConsumer(consumer);
    }

    public void run() {
        for (ConsumerManager<T> manager : consumers.values()) {
            System.out.println(manager);
            for (Consumer<T> consumer : manager.getConsumers()) {
                ConsumerRunner<T> runner = new ConsumerRunner<>(consumer);
                runner.run();
            }
        }
    }

    public MessageQueue<Message<T>> getQueue(String queueName) {
        return queues.get(queueName);
    }

    public DeadLetterQueue<Message<T>> getDeadLetterQueue() {
        return this.deadLetterQueue;
    }

    public Map<String, ConsumerManager<T>> getConsumers() {
        return this.consumers;
    }

    public String getMessageStorePath() {
        return messageStorePath;
    }
}
