package org.kittenmq.connections;

import org.kittenmq.brokers.Broker;
import org.kittenmq.errors.ErrorHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectionManager {
    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();
    private final int maxRetries;
    private final int retryDelay;

    public ConnectionManager() {
        this.maxRetries = 3;
        this.retryDelay = 1000;
    }

    public ConnectionManager(int maxRetries, int retryDelay) {
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
    }

    public Connection createConnection(String clientId, Broker broker) {
        int retryCount = 0;

        while (retryCount < this.maxRetries) {
            try {
                Connection connection = new Connection(clientId, broker);
                connections.put(clientId, connection);
                return connection;
            } catch (Exception e) {
                retryCount++;
                ErrorHandler.logWarning("Connection attempt " + retryCount + " failed. Retrying...");
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        throw new RuntimeException("Failed to establish connection after " + this.maxRetries + " attempts");
    }

    public void closeConnection(String clientId) {
        connections.remove(clientId);
    }

    public Connection getConnection(String clientId) {
        return connections.get(clientId);
    }
}
