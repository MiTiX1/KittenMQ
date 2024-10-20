package org.kittenmq.messages;

public class AcknowledgmentEvent<T> {
    private final T message;

    public AcknowledgmentEvent(T message) {
        this.message = message;
    }

    public T getMessage() {
        return message;
    }
}
