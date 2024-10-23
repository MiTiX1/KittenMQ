# KittenMQ

**KittenMQ** is a lightweight message broker service, built for simplicity and ease of use, designed to handle the communication between producers and consumers via message queues. This message broker enables you to send, receive, and process messages.

## Features

- **Queue-based message routing**: Supports multiple message queues for routing messages from producers to consumers.
- **Multiple producers**: You can register multiple producers that send messages to different queues.
- **Multiple consumers**: Consumers can subscribe to specific queues and process incoming messages.
- **Dead-letter queue support**: Unprocessed messages are stored in a dead-letter queue for troubleshooting.
- **Custom message processing**: You can define custom handlers (callbacks) for processing messages.

## Getting Started

This guide walks you through setting up and using KittenMQ in a Java project.

### Prerequisites

- **Java 23+**
- Basic understanding of producer-consumer patterns
- Maven for dependency management

### Installation

Clone the repository.
```shell
git clone https://github.com/MiTiX1/KittenMQ.git
```

### Example Usage

Here’s an example of how to set up and use KittenMQ with producers, consumers, and message queues.

```java
public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Initialize the broker
        Broker<TestMessage> broker = new Broker<>();

        // Create message queues
        MessageQueue<Message<TestMessage>> queue1 = new MessageQueue<>("queue1", broker.getDeadLetterQueue(), broker.getMessageStorePath());

        // Create producers
        Producer<TestMessage> producer1 = new Producer<>("producer1", broker, "queue1");

        // Create consumers and assign message handler callbacks
        Consumer<TestMessage> consumer1 = new Consumer<>("consumer1", broker, "queue1", Main::processMessage, 5000);
        Consumer<TestMessage> consumer2 = new Consumer<>("consumer2", broker, "queue1", Main::processMessage, 5000);

        // Create load balancer and register the consumers
        RoundRobinLoadBalancer<Message<TestMessage>> loadBalancer = new RoundRobinLoadBalancer<>(queue1);
        loadBalancer.registerConsumer(consumer1);
        loadBalancer.registerConsumer(consumer2);

        // Register queues and consumers to the broker
        broker.registerQueue(queue1);
        broker.registerConsumer(consumer1);
        broker.registerConsumer(consumer2);
        broker.registerLoadBalancer(loadBalancer);

        // Start the broker service
        broker.run();

        // Send messages via producers
        for (int i = 0; i < 5; i++) {
            producer1.sendMessage(new TestMessage("message: " + i, "john", "bob"));
        }
    }

    // Message handler function
    public static void processMessage(Message<TestMessage> message) {
        System.out.println("message: " + message.getPayload());
    }
}
```

Here’s an example of what the output look like when running the above program:

```shell
---consumer1---
message: TestMessage{message='message: 0', sender='john', receiver='bob'}
---consumer2---
message: TestMessage{message='message: 1', sender='john', receiver='bob'}
---consumer1---
message: TestMessage{message='message: 2', sender='john', receiver='bob'}
---consumer2---
message: TestMessage{message='message: 3', sender='john', receiver='bob'}
---consumer1---
message: TestMessage{message='message: 4', sender='john', receiver='bob'}
```
