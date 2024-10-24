package org.kittenmq.persistence;

import com.google.gson.Gson;
import org.kittenmq.errors.ErrorHandler;
import org.kittenmq.messages.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MessageStore<T> {
    private final String db_url;

    public MessageStore(String db_path) {
        this.db_url = "jdbc:sqlite:" + db_path + "/database.db";
        this.createFolderIfNotExists(db_path);
        this.initDatabase();
    }

    private void createFolderIfNotExists(String folderPath) {
        Path path = Paths.get(folderPath);

        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                ErrorHandler.logError("Could not create folder: " + folderPath, e);
            }
        }
    }

    private void initDatabase() {
        String createMessagesTable = "CREATE TABLE IF NOT EXISTS messages (" +
                "uuid TEXT PRIMARY KEY," +
                "timestamp INTEGER NOT NULL," +
                "headers TEXT NOT NULL," +
                "payload BLOB NOT NULL," +
                "acknowledged INTEGER NOT NULL DEFAULT 0);";

        String createDeadLettersTable = "CREATE TABLE IF NOT EXISTS dead_letters (" +
                "uuid TEXT PRIMARY KEY," +
                "timestamp INTEGER NOT NULL," +
                "headers TEXT NOT NULL," +
                "payload BLOB NOT NULL);";

        try (Connection conn = DriverManager.getConnection(this.db_url);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createMessagesTable);
            stmt.execute(createDeadLettersTable);
        } catch (SQLException e) {
            ErrorHandler.logError("Database initialization error", e);
        }
    }

    public void storeMessage(Message<T> message) {
        String sql = "INSERT INTO messages (uuid, timestamp, headers, payload, acknowledged) VALUES (?, ?, ?, ?, ?)";

        try {
            Connection conn = DriverManager.getConnection(this.db_url);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, message.getUuid().toString());
            pstmt.setLong(2, message.getTimestamp());
            pstmt.setString(3, this.serializeHeaders(message.getHeaders()));
            pstmt.setBytes(4, this.serializePayload(message.getPayload()));
            pstmt.setInt(5, message.isAcknowledged() ? 1 : 0);
            pstmt.executeUpdate();
        } catch (SQLException | IOException e) {
            ErrorHandler.logError("Error saving the message", e);
        }
    }

    public List<Message<T>> loadMessages() {
        List<Message<T>> messages = new ArrayList<>();
        String sql = "SELECT * FROM messages WHERE acknowledged = 0";
        try {
            Connection conn = DriverManager.getConnection(this.db_url);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                UUID uuid = UUID.fromString(rs.getString("uuid"));
                long timestamp = rs.getLong("timestamp");
                String headersJson = rs.getString("headers");
                Map<String, String> headers = this.deserializeHeaders(headersJson);
                T payload = this.deserializePayload(rs.getBytes("payload"));
                boolean acknowledged = rs.getInt("acknowledged") == 1;
                Message<T> message = new Message<>(uuid, timestamp, headers, payload, acknowledged);
                messages.add(message);
            }
        } catch (SQLException | IOException | ClassNotFoundException e) {
            ErrorHandler.logError("Error loading messages", e);
        }
        return messages;
    }

    public void acknowledgeMessage(UUID messageId) {
        String sql = "UPDATE messages SET acknowledged = 1 WHERE uuid = ?";

        try {
            Connection conn = DriverManager.getConnection(this.db_url);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, messageId.toString());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            ErrorHandler.logError("Error acknowledging message", e);
        }
    }

    public void moveToDeadLetters(Message<T> message) {
        String insertSql = "INSERT INTO dead_letters (uuid, timestamp, headers, payload) VALUES (?, ?, ?, ?)";
        String deleteSql = "DELETE FROM messages WHERE uuid = ?";

        try {
            Connection conn = DriverManager.getConnection(this.db_url);
            PreparedStatement insertStmt = conn.prepareStatement(insertSql);
            PreparedStatement deleteStmt = conn.prepareStatement(deleteSql);

            insertStmt.setString(1, message.getUuid().toString());
            insertStmt.setLong(2, message.getTimestamp());
            insertStmt.setString(3, this.serializeHeaders(message.getHeaders()));
            insertStmt.setBytes(4, serializePayload(message.getPayload()));
            insertStmt.executeUpdate();

            deleteStmt.setString(1, message.getUuid().toString());
            deleteStmt.executeUpdate();
        } catch (SQLException | IOException e) {
            ErrorHandler.logError("Error moving dead letters", e);
        }
    }

    private byte[] serializePayload(T payload) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(payload);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private T deserializePayload(byte[] payloadBytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payloadBytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        }
    }

    private String serializeHeaders(Map<String, String> headers) {
        Gson gson = new Gson();
        return gson.toJson(headers);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> deserializeHeaders(String headersJson) {
        Gson gson = new Gson();
        return (Map<String, String>) gson.fromJson(headersJson, Map.class);
    }
}
