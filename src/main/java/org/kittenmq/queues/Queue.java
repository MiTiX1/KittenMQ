package org.kittenmq.queues;

import org.kittenmq.messages.Message;

import java.io.IOException;

public interface Queue<T> {
    public void enqueue(Message<T> message) throws InterruptedException, IOException;
    public Message<T> dequeue() throws InterruptedException;
    public boolean isEmpty();
    public String getName();
}
