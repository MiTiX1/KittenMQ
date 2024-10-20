package org.kittenmq.messages;

public class AcknowledgmentEvent<T> {
    private final Message<T> message;

    public AcknowledgmentEvent(Message<T> message) {
        this.message = message;
    }

    public Message<T> getMessage() {
        return this.message;
    }
}
