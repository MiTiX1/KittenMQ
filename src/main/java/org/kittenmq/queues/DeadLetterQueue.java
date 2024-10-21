package org.kittenmq.queues;

import org.kittenmq.messages.Message;
import org.kittenmq.messages.MessageStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DeadLetterQueue<T> implements Queue<T>{
    private final BlockingQueue<Message<T>> queue = new LinkedBlockingQueue<>();
    private final MessageStore<Message<T>> messageStore;
    private final String name;

    public DeadLetterQueue(String name, String messageStorePath) {
        this.name = name;
        this.messageStore = new MessageStore<>(Paths.get(messageStorePath, this.name + ".dat").toString());;
    }

    public void enqueue(Message<T> message) throws InterruptedException, IOException {
        this.queue.put(message);
        messageStore.save(message);
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
