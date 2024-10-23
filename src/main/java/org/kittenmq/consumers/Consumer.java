package org.kittenmq.consumers;


import org.kittenmq.brokers.Broker;
import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.ConsumerMessageQueue;
import org.kittenmq.queues.MessageQueue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Consumer<T>{
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY = 1000;
    private static final long TIMEOUT = 600_000L;
    private final String name;
    private final Broker<T> broker;
    private final String queueName;
    private final ConsumerCallback<T> callback;
    private final int maxRetries;
    private final int retryDelay;
    private final long timeout;
    private final ConsumerMessageQueue<T> queue = new ConsumerMessageQueue<>();
    private boolean isRunning = false;

    public Consumer(String name, Broker<T> broker, String queueName, ConsumerCallback<T> callback, int maxRetries, int retryDelay, long timeout) {
        this.name = name;
        this.broker = broker;
        this.queueName = queueName;
        this.callback = callback;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        this.timeout = timeout;
    }

    public Consumer(String name, Broker<T> broker, String queueName, ConsumerCallback<T> callback, int maxRetries, int retryDelay) {
        this(name, broker, queueName, callback, maxRetries, retryDelay, Consumer.TIMEOUT);
    }

    public Consumer(String name, Broker<T> broker, String queueName, ConsumerCallback<T> callback, int timeout) {
        this(name, broker, queueName, callback, Consumer.MAX_RETRIES, Consumer.RETRY_DELAY, timeout);
    }

    public Consumer(String name, Broker<T> broker, String queueName, ConsumerCallback<T> callback) {
        this(name, broker, queueName, callback, Consumer.MAX_RETRIES, Consumer.RETRY_DELAY, Consumer.MAX_RETRIES);
    }

    @SuppressWarnings("unchecked")
    public void consume() throws InterruptedException, IOException {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + this.timeout;
        this.isRunning = true;

        while (System.currentTimeMillis() < endTime) {
            int retryCount = 0;
            boolean success = false;
            Message<T> message = this.queue.dequeue(1000, TimeUnit.MILLISECONDS);

            if (message != null) {
                while (retryCount < this.maxRetries) {
                    try {
                        System.out.println("---" + this.getName() + "---");
                        callback.process(message);
                        this.broker.getQueue(this.queueName).processAcknowledgment((Message<Message<T>>) message);
                        success = true;
                        endTime = System.currentTimeMillis() + this.timeout;
                        break;
                    } catch (Exception e) {
                        retryCount++;
                        ErrorHandler.logError("Error processing message. Retry " + retryCount, e);
                        Thread.sleep(this.retryDelay);
                    }
                }
                if (!success) {
                    ErrorHandler.logWarning("Failed to process message after " + this.maxRetries + " retries.");
                    this.broker.getQueue(this.queueName).moveToDeadLetterQueue((Message<Message<T>>) message);
                }
            }
        }
        this.isRunning = false;
        ErrorHandler.logWarning("Timeout reached without receiving a message.");
        ErrorHandler.logWarning("Shutting down...");
        Thread.currentThread().interrupt();
    }

    public ConsumerMessageQueue<T> getQueue() {
        return this.queue;
    }

    public boolean isRunning() {
        return this.isRunning;
    }

    public String getName() {
        return this.name;
    }

    public String getQueueName() {
        return this.queueName;
    }
}
