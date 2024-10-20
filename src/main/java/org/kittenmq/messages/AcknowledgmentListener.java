package org.kittenmq.messages;

public interface AcknowledgmentListener<T> {
    void onMessageAcknowledged(AcknowledgmentEvent<T> event);
}
