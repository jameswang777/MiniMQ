package io.github.jameswang777.minimq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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
     *
     * @param topic   The destination topic.
     * @param message The message object.
     * @return The unique message ID assigned by the broker.
     */
    public String send(String topic, Object message) {
        String messageContent;
        try {
            messageContent = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize message object for topic '{}'", topic, e);
            throw new RuntimeException("Message serialization failed", e);
        }

        // Implement retry logic
        int attempts = 0;
        Exception lastException = null;

        while (attempts < producerProps.getRetries()) {
            attempts++;
            Socket socket = null;
            try {
                socket = connectionManager.borrowConnection();
                String command = String.format("PRODUCE:%s:%s%n", topic, messageContent);

                // 获取输入和输出流
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                // 1. 发送 PRODUCE 命令
                out.print(command);
                out.flush(); // Ensure data is sent immediately
                // 2. 等待并读取 Broker 的响应 (即 Message ID)
                String messageId = in.readLine();
                // 3. 归还连接并返回 ID
                connectionManager.returnConnection(socket);
                log.debug("Successfully sent message to topic '{}', received messageId [{}].", topic, messageId);
                return messageId;
            } catch (Exception e) {
                log.warn("Failed to send message to topic '{}' on attempt {}. Retrying...", topic, attempts, e);
                lastException = e;
                connectionManager.invalidateConnection(socket); // 销毁坏掉的连接
                if (attempts < producerProps.getRetries()) {
                    try {
                        Thread.sleep(producerProps.getRetryDelayMs());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry delay was interrupted", ie);
                    }
                }
            }
        }
        // 如果所有重试都失败了，抛出最终的异常
        log.error("Failed to send message to topic '{}' after {} attempts.", topic, producerProps.getRetries());
        throw new RuntimeException("Failed to send message after all retries", lastException);
    }

}