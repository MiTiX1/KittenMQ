package org.kittenmq.messages;

import org.kittenmq.errors.ErrorHandler;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MessageStore<T> {
    private final String filePath;

    public MessageStore(String filePath) {
        this.filePath = filePath;

        try {
            File file = new File(filePath);
            Files.createDirectories(file.getParentFile().toPath());
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            ErrorHandler.logError("Error initializing message store", e);
        }
    }

    public List<Message<T>> loadAll() throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            return new ArrayList<>();
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            return (List<Message<T>>) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to read messages: " + e.getMessage());
        }
    }

    public void save(Message<T> message) throws IOException {
        List<Message<T>> messages = new ArrayList<>();
        messages.add(message);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(messages);
        }
    }

    public void clear() throws IOException {
        new File(filePath).delete();
        new File(filePath).createNewFile();
    }
}
