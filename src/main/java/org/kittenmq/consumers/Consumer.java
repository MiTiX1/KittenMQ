package org.kittenmq.consumers;


import org.kittenmq.brokers.Broker;
import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.AcknowledgmentEvent;
import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageQueue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Consumer<T> {
    private final String name;
    private final Broker broker;
    private final String queueName;

    public Consumer(String name, Broker broker, String queueName) {
        this.name = name;
        this.broker = broker;
        this.queueName = queueName;
    }

    public void consume(ConsumerCallback<T> callback) throws InterruptedException {
        MessageQueue queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }

        while (true) {
            Message message =  queue.dequeue();
            callback.process(message);
            AcknowledgmentEvent event = new AcknowledgmentEvent<>(message);
            queue.processAcknowledgment(event);
        }
    }

    public void consume(ConsumerCallback<T> callback, int maxRetries, int retryDelay) throws InterruptedException {
        MessageQueue queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }

        while (true) {
            try {
                Message message =  queue.dequeue();
                if (message == null) {
                    continue;
                }

                int retryCount = 0;
                boolean success = false;

                while (retryCount < maxRetries) {
                    try {
                        callback.process(message);
                        AcknowledgmentEvent event = new AcknowledgmentEvent<>(message);
                        queue.processAcknowledgment(event);
                        success = true;
                        break;
                    } catch (Exception e) {
                        retryCount++;
                        ErrorHandler.logError("Error processing message. Retry " + retryCount, e);
                        Thread.sleep(retryDelay);
                    }
                }

                if (!success) {
                    ErrorHandler.logWarning("Failed to process message after " + maxRetries + " retries.");
                    queue.moveToDeadLetterQueue(message);
                }
            } catch (Exception e) {
                ErrorHandler.logError("Unexpected error during consumption", e);
            }
        }
    }

    public void consume(ConsumerCallback<T> callback, long timeout) throws InterruptedException, IOException {
        MessageQueue queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }

        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeout;

        while (System.currentTimeMillis() < endTime) {
            Message message = queue.dequeue(timeout, TimeUnit.MILLISECONDS);

            if (message != null) {
                try {
                    callback.process(message);
                    AcknowledgmentEvent event = new AcknowledgmentEvent<>(message);
                    queue.processAcknowledgment(event);
                    endTime = System.currentTimeMillis() + timeout;
                } catch (Exception e) {
                    ErrorHandler.logError("Error processing message", e);
                    queue.moveToDeadLetterQueue(message);
                }
            } else {
                ErrorHandler.logWarning("Timeout reached without receiving a message.");
            }
        }
    }

    public void consume(ConsumerCallback<T> callback, int maxRetries, int retryDelay, long timeout) throws InterruptedException, IOException {
        MessageQueue queue = broker.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue does not exist: " + queueName);
        }

        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeout;

        while (System.currentTimeMillis() < endTime) {
            Message message = queue.dequeue(timeout, TimeUnit.MILLISECONDS);

            if (message != null) {
                int retryCount = 0;
                boolean success = false;

                while (retryCount < maxRetries) {
                    try {
                        callback.process(message);
                        AcknowledgmentEvent event = new AcknowledgmentEvent<>(message);
                        queue.processAcknowledgment(event);
                        success = true;
                        endTime = System.currentTimeMillis() + timeout;
                        break;
                    } catch (Exception e) {
                        retryCount++;
                        ErrorHandler.logError("Error processing message. Retry " + retryCount, e);
                        Thread.sleep(retryDelay);
                    }
                }
                if (!success) {
                    ErrorHandler.logWarning("Failed to process message after " + maxRetries + " retries.");
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
}
