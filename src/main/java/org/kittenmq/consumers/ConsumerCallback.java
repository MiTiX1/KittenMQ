package org.kittenmq.consumers;

import org.kittenmq.messages.Message;

public interface ConsumerCallback<T> {
    void process(Message<T> message);
}
