package org.kittenmq.messages;

import org.kittenmq.errors.ErrorHandler;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageQueue<T> {
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();;
    private final MessageQueue<T> deadLetterQueue;
    private final MessageStore<T> messageStore;
    private final String name;

    public MessageQueue(String name, MessageQueue<T> deadLetterQueue, String messageStorePath) {
        this.name = name;
        this.deadLetterQueue = deadLetterQueue;
        this.messageStore = new MessageStore<>(Paths.get(messageStorePath, this.name + ".dat").toString());
    }

    public void enqueue(T message) throws InterruptedException {
        this.queue.put(message);
    }

    public T dequeue() throws InterruptedException {
        return this.queue.take();
    }

    public T dequeue(long timeout, TimeUnit unit) throws InterruptedException {
        return this.queue.poll(timeout, unit);
    }

    public void moveToDeadLetterQueue(T message) throws InterruptedException {
        if (this.deadLetterQueue != null) {
            this.deadLetterQueue.enqueue(message);
        }
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    private void loadMessages() {
        try {
            List<Message<T>> messages = this.messageStore.loadAll();
            for (Message<T> message : messages) {
                this.queue.put((T) message);
            }
        } catch (IOException | InterruptedException e) {
            ErrorHandler.logError("Error loading messages", e);
        }
    }

    public String getName() {
        return this.name;
    }
}
