package org.kittenmq.connections;

import org.kittenmq.brokers.Broker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectionManager {
    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();

    public Connection createConnection(String clientId, Broker broker) {
        Connection connection = new Connection(clientId, broker);
        connections.put(clientId, connection);
        return connection;
    }

    public void closeConnection(String clientId) {
        connections.remove(clientId);
    }

    public Connection getConnection(String clientId) {
        return connections.get(clientId);
    }
}
