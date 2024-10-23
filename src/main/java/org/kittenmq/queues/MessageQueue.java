package org.kittenmq.queues;

import org.kittenmq.messages.Message;
import org.kittenmq.persistence.MessageStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageQueue<T> implements Queue<T> {
    private final BlockingQueue<Message<T>> queue = new LinkedBlockingQueue<>();
    private final DeadLetterQueue<T> deadLetterQueue;
    private final MessageStore<T> messageStore;
    private final String name;

    public MessageQueue(String name, DeadLetterQueue<T> deadLetterQueue, String messageStorePath) {
        this.name = name;
        this.deadLetterQueue = deadLetterQueue;
        this.messageStore = new MessageStore<>(messageStorePath);
        this.loadMessages();
    }

    public void enqueue(Message<T> message) throws InterruptedException, IOException {
        this.messageStore.storeMessage(message);
        this.queue.put(message);
    }

    public Message<T> dequeue() throws InterruptedException {
        return this.queue.take();
    }

    public Message<T> dequeue(long timeout, TimeUnit unit) throws InterruptedException {
        return this.queue.poll(timeout, unit);
    }

    public void moveToDeadLetterQueue(Message<T> message) throws InterruptedException, IOException {
        if (this.deadLetterQueue != null) {
            this.deadLetterQueue.enqueue(message);
        }
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    private void loadMessages() {
        List<Message<T>> messages = this.messageStore.loadMessages();
        if (messages != null && !messages.isEmpty()) {
            queue.addAll(messages);
        }
    }

    public void processAcknowledgment(Message<T> message) {
        this.messageStore.acknowledgeMessage(message.getUuid());
    }

    public String getName() {
        return this.name;
    }
}
