import java.io.Serializable;

public class TestMessage implements Serializable {
    private final String message;
    private final String sender;
    private final String receiver;

    public TestMessage(String message, String sender, String receiver) {
        this.message = message;
        this.sender = sender;
        this.receiver = receiver;
    }

    @Override
    public String toString() {
        return "TestMessage{" +
                "message='" + message + '\'' +
                ", sender='" + sender + '\'' +
                ", receiver='" + receiver + '\'' +
                '}';
    }
}
