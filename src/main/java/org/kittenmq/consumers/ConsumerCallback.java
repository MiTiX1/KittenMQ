package org.kittenmq.consumers;

public interface ConsumerCallback<T> {
    void process(T message);
}
