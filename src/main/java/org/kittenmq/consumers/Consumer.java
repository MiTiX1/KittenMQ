package org.kittenmq.consumers;


import org.kittenmq.brokers.Broker;
import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.AcknowledgmentEvent;
import org.kittenmq.messages.Message;
import org.kittenmq.queues.MessageQueue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Consumer<T> {
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

    public void consume() throws InterruptedException, IOException {
        MessageQueue queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }

        long startTime = System.currentTimeMillis();
        long endTime = startTime + this.timeout;

        while (System.currentTimeMillis() < endTime) {
            Message message = queue.dequeue(this.timeout, TimeUnit.MILLISECONDS);

            if (message != null) {
                int retryCount = 0;
                boolean success = false;

                while (retryCount < this.maxRetries) {
                    try {
                        System.out.println("---" + this.getName() + "---");
                        callback.process(message);
                        AcknowledgmentEvent event = new AcknowledgmentEvent<>(message);
//                        queue.processAcknowledgment(event);
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
                    queue.moveToDeadLetterQueue(message);
                }
            } else {
                ErrorHandler.logWarning("Timeout reached without receiving a message.");
            }
        }
    }

    public String getName() {
        return this.name;
    }

    public String getQueueName() {
        return this.queueName;
    }
}
