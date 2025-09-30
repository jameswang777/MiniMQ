package io.github.jameswang777.minimq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.net.Socket;

@Slf4j
public class MiniMqTemplate {

    private final ConnectionManager connectionManager;
    private final ObjectMapper objectMapper;
    private final MiniMqProperties.Producer producerProps;

    public MiniMqTemplate(ConnectionManager connectionManager, ObjectMapper objectMapper, MiniMqProperties properties) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
        this.producerProps = properties.getProducer();
    }

    /**
     * Sends a message to the specified topic.
     * The message object will be serialized to JSON.
     * @param topic The destination topic.
     * @param message The message object.
     */
    public void send(String topic, Object message) {
        String messageContent;
        try {
            messageContent = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize message object for topic '{}'", topic, e);
            throw new RuntimeException("Message serialization failed", e);
        }

        // Implement retry logic
        int attempts = 0;
        boolean sent = false;
        while (attempts < producerProps.getRetries() && !sent) {
            attempts++;
            Socket socket = null;
            try {
                socket = connectionManager.borrowConnection();
                String command = String.format("PRODUCE:%s:%s%n", topic, messageContent);

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.print(command);
                out.flush(); // Ensure data is sent immediately

                connectionManager.returnConnection(socket);
                sent = true;
                log.debug("Successfully sent message to topic '{}' on attempt {}", topic, attempts);
            } catch (Exception e) {
                log.warn("Failed to send message to topic '{}' on attempt {}. Retrying...", topic, attempts, e);
                connectionManager.invalidateConnection(socket); // Invalidate faulty connection
                if (attempts >= producerProps.getRetries()) {
                    log.error("Failed to send message to topic '{}' after {} attempts.", topic, producerProps.getRetries());
                    throw new RuntimeException("Failed to send message after retries", e);
                }
                try {
                    Thread.sleep(producerProps.getRetryDelayMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry delay was interrupted", ie);
                }
            }
        }
    }
}