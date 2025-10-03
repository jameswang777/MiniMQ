---
title: MiniMQ
---

# MiniMQ

Java 17+ | Python 3.7+ | Build with Maven & Pip | MIT License

**MiniMQ** is a powerful, educational message queue system built from the ground up. It demonstrates the core principles of modern messaging systems, focusing on reliability, high performance, and a superb developer experience. The project includes a robust Java-based Broker, a feature-rich Spring Boot Starter, and a clean, installable Python client.

## ✨ Core Features

- **Reliable Messaging**: Guarantees "at-least-once" delivery with a Write-Ahead Log (WAL) for persistence and a full ACK confirmation mechanism.
- **High Performance**: Utilizes a TCP-based binary protocol and a high-performance connection pool (Apache Commons Pool2) to minimize latency.
- **Modern Java Client**: A full-featured Spring Boot 3.x Starter that provides:
    - Auto-configuration for all components.
    - A simple MiniMqTemplate for producing messages with built-in retries.
    - An elegant @MiniMqListener annotation for creating consumers effortlessly.
- **Idiomatic Python Client**: A standalone, installable Python package (minimq_client) with:
    - A simple MiniMqProducer for sending messages.
    - A decorator-based @consumer.listener for creating asynchronous consumers.
- **JSON Serialization**: Seamlessly handles serialization and deserialization of message objects (POJOs/DTOs in Java, Dictionaries in Python).

## 📂 Project Structure

The project is organized as a multi-module repository, making it easy to navigate and maintain.

- **MiniMQ/** (项目总根目录)
-  **mq-broker-server/** (核心 Java Broker 服务器, Maven 模块)
-  **clients/** (存放所有客户端代码)
-  **java/**
-  **mq-client-spring-boot-starter/** (Java Spring Boot Starter, Maven 模块)
-  **python/**
-  **mq-client-python-package/** (Python 客户端库, 可安装包)
-  **examples/** (存放所有示例应用程序)
-  **example-java/** (Spring Boot 示例程序)
-  **example-python/** (Python 示例程序)

## 🚀 Quick Start: Running the System

You need three components running: the Broker, a Producer, and a Consumer.

### 1. Run the Broker Server

The Broker is the central hub. It must be running first.

```bash
Navigate to the broker module

cd mq-broker-server
Build and run using Maven (recommended)

mvn clean spring-boot:run
Or build a JAR and run it
mvn clean package
java -jar target/mq-broker-server-1.0.0-SNAPSHOT.jar

``` 
The Broker will start on `localhost:5677`.

---
## ☕ Java (Spring Boot) Client Guide

### 1. Install the Starter

First, build and install the starter to your local Maven repository.

```bash
Navigate to the starter module

cd clients/java/mq-client-spring-boot-starter
Install it locally

mvn clean install 
```

### 2. Add Dependency

In your Spring Boot application, add the starter to your `pom.xml`:

```xml 
io.github.jameswang777.minimq mq-client-spring-boot-starter 1.0.0-SNAPSHOT 
```

### 3. Producing Messages

Inject the `MiniMqTemplate` and send any serializable object.

```java 

@RestController 
public class OrderController { 

@Autowired 
private MiniMqTemplate MiniMqTemplate;

@GetMapping("/order")
public String createOrder() {
    OrderDto order = new OrderDto("order-123", "Laptop", 1);
    String messageId = MiniMqTemplate.send("ORDER_TOPIC", order);
    return "Order sent! && messageId: " + messageId;
}

}
```
### 4. Consuming Messages

Use the `@MiniMqListener` annotation on any method in a Spring component.

```java 
@Component public class OrderConsumer {

    @MiniMqListener(topic = "ORDER_TOPIC")
    public void handleOrder(OrderDto order) {
        // The 'order' object is automatically deserialized from JSON!
        System.out.println("Received new order: " + order.getOrderId());
    }

    @MiniMqListener(topic = "ORDER_TOPIC")
    public void handleOrderWithId(OrderDto order, @Header(MiniMqHeaders.MESSAGE_ID) String messageId) {
        System.out.println("Broker Message ID from @Header: " + messageId);
    }
} 

``` 
See the `examples/example-app` module for a complete, runnable example.

---
## 🐍 Python Client Guide

### 1. Build and Install the Package

First, build the Python package into a wheel (`.whl`) file.

```bash
Navigate to the Python package root

cd python/mq-client-python-package
Install build tools (if you haven't already)

pip install build
Build the package

python -m build 
``` 
This creates a `.whl` file in the `dist/` directory.

### 2. Install the Package

In your Python application's environment, install the package from the generated wheel file.

```bash
The path will be relative to your project

pip install python/mq-client-python-package/dist/minimq_client-1.0.0-py3-none-any.whl 
```


### 3. Producing Messages

Instantiate the `MiniMqProducer` and send a dictionary.

```python 
from minimq_client import MiniMqProducer

producer = MiniMqProducer(host="localhost", port=5677)

event_data = { "user_id": "user-456", "event_type": "login" } 
producer.send("USER_EVENTS", event_data) 
print("Event sent!") 
```

### 4. Consuming Messages

Use the `@consumer.listener` decorator.

```python from minimq_client import MiniMqConsumer

consumer = MiniMqConsumer(host="localhost", port=5677)

@consumer.listener(topic="USER_EVENTS") def handle_user_events(message: dict):

# The 'message' is automatically deserialized from JSON
print(f"Received user event: {message}")

This will start the consumer and block until you press Ctrl+C

consumer.start() 
``` 
See the `examples/example-python` module for a complete, runnable example.

## 🤝 Contributing

This project is a journey in building a distributed system from first principles. Contributions, ideas, and bug reports are highly welcome. Please feel free to fork the repository, make your changes, and submit a pull request.

## 📜 License

This project is licensed under the **MIT License**. See the `LICENSE` file for details.