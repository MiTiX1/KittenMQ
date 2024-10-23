package org.kittenmq.messages;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Message<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final UUID uuid;
    private final long timestamp;
    private final Map<String, String> headers;
    private final T payload;
    private boolean acknowledged;

    public Message(T payload) {
        this(UUID.randomUUID(), System.currentTimeMillis(), new HashMap<>(), payload, false);
    }

    public Message(T payload, Map<String, String> headers) {
        this(UUID.randomUUID(), System.currentTimeMillis(), headers, payload, false);
    }

    public Message(UUID uuid, long timestamp, Map<String, String> headers, T payload, boolean acknowledged) {
        this.uuid = uuid;
        this.timestamp = timestamp;
        this.headers = headers;
        this.payload = payload;
        this.acknowledged = acknowledged;
    }

    public void addHeader(String key, String value) {
        this.headers.put(key, value);
    }

    public UUID getUuid() {
        return this.uuid;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public T getPayload() {
        return this.payload;
    }

    public boolean isAcknowledged() {
        return this.acknowledged;
    }

    public void acknowledge() {
        this.acknowledged = true;
    }
}
