package org.kittenmq.queues;

import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.AcknowledgmentEvent;
import org.kittenmq.messages.AcknowledgmentListener;
import org.kittenmq.messages.Message;
import org.kittenmq.persistence.MessageStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageQueue<T> implements Queue<T> {
    private final BlockingQueue<Message<T>> queue = new LinkedBlockingQueue<>();
    private final DeadLetterQueue<T> deadLetterQueue;
    private final MessageStore<T> messageStore;
    private final List<AcknowledgmentListener<T>> acknowledgmentListeners = new ArrayList<>();
    private final String name;

    public MessageQueue(String name, DeadLetterQueue<T> deadLetterQueue, String messageStorePath) {
        this.name = name;
        this.deadLetterQueue = deadLetterQueue;
        this.messageStore = new MessageStore<>(messageStorePath);
        this.loadMessages();
    }

    public void enqueue(Message<T> message) throws InterruptedException, IOException {
        this.queue.put(message);
        messageStore.storeMessage(message);
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

    public void registerAcknowledgmentListener(AcknowledgmentListener<T> listener) {
        acknowledgmentListeners.add(listener);
    }

    public void processAcknowledgment(AcknowledgmentEvent<T> event) {
        for (AcknowledgmentListener<T> listener : acknowledgmentListeners) {
            listener.onMessageAcknowledged(event);
        }
        this.messageStore.acknowledgeMessage(event.getMessage().getUuid());
    }

    public String getName() {
        return this.name;
    }
}
