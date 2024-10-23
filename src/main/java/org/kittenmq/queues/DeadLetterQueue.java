package org.kittenmq.queues;

import org.kittenmq.messages.Message;
import org.kittenmq.persistence.MessageStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DeadLetterQueue<T> implements Queue<T>{
    private final BlockingQueue<Message<T>> queue = new LinkedBlockingQueue<>();
    private final MessageStore<Message<T>> messageStore;
    private final String name;

    public DeadLetterQueue(String name, String messageStorePath) {
        this.name = name;
        this.messageStore = new MessageStore<>(messageStorePath);
    }

    public void enqueue(Message<T> message) throws InterruptedException, IOException {
        this.queue.put(message);
        messageStore.moveToDeadLetters((Message<Message<T>>) message);
    }

    public Message<T> dequeue() throws InterruptedException {
        return this.queue.take();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public String getName() {
        return name;
    }
}