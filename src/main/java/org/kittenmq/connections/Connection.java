package org.kittenmq.connections;

import org.kittenmq.brokers.Broker;

public class Connection {
    private final String name;
    private final Broker broker;

    public Connection(String name, Broker broker) {
        this.name = name;
        this.broker = broker;
    }

    public Broker getBroker() {
        return broker;
    }

    public String getName() {
        return this.name;
    }
}
