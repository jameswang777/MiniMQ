package io.github.jameswang777.minimq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import io.github.jameswang777.minimq.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

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
     * @param payload The message object.
     * @return The unique message ID assigned by the broker.
     */
    public String send(String topic, Object payload) {
        String messageContent;
        try {
            // 1. 将业务对象 payload 序列化为 JSON 字符串
            messageContent = objectMapper.writeValueAsString(payload);
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
                // 2. 使用 Message 构造器创建异步消息对象
                Message message = new Message(topic, messageContent);
                // 3. 使用 Message.toString() 方法进行协议序列化
                String serializedMessage = message.toString();
                String command = "PRODUCE:" + serializedMessage + "\n"; // 遵循 v1.1 协议
                log.debug("Sending command: {}", command);

                socket = connectionManager.borrowConnection();
                // 获取输入和输出流
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                // 发送 PRODUCE 命令
                out.print(command);
                out.flush(); // Ensure data is sent immediately
                // 等待并读取 Broker 的响应 (即 Message ID)
                String messageId = in.readLine();
                // 归还连接并返回 ID
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

    /**
     * 发送一个请求消息，并同步阻塞等待回复。
     *
     * @param topic          请求要发送到的目标主题。
     * @param requestPayload 请求的业务数据对象。
     * @param responseType   期望的响应业务数据对象的 Class 类型。
     * @param timeoutMillis  等待响应的超时时间（毫-秒）。
     * @param <T>            响应业务数据对象的泛型。
     * @return 反序列化后的响应业务数据对象。
     * @throws TimeoutException 如果在指定时间内未收到匹配的响应。
     * @throws RuntimeException 如果发生网络或其他严重错误。
     */
    public <T> T requestReply(String topic, Object requestPayload, Class<T> responseType, long timeoutMillis) throws TimeoutException {
        // 1. 准备请求消息的元数据
        String correlationId = UUID.randomUUID().toString();
        String replyToTopic = "reply.temp." + UUID.randomUUID(); // 动态生成唯一的回复主题

        Socket socket = null;
        try {
            // 2. 准备请求消息
            String requestContent = objectMapper.writeValueAsString(requestPayload);
            Message requestMessage = new Message(topic, requestContent, correlationId, replyToTopic);
            String serializedRequest = requestMessage.toString();
            String produceCommand = "PRODUCE:" + serializedRequest + "\n";

            // 3. 建立通信并发送请求
            socket = connectionManager.borrowConnection();
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            log.debug("Sending request with correlationId [{}], replyTo [{}].", correlationId, replyToTopic);
            out.print(produceCommand);
            out.flush();
            in.readLine(); // 读取并忽略 PRODUCE 的 "MessageID" 响应

            // 4. 循环消费，等待响应
            String consumeCommand = "CONSUME:" + replyToTopic + "\n";
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < timeoutMillis) {
                out.print(consumeCommand);
                out.flush();
                String rawResponse = in.readLine();

                if (rawResponse != null && !rawResponse.equals("NO_MSG")) {
                    log.debug("Received potential reply on [{}]: {}", replyToTopic, rawResponse);
                    Message responseMessage = Message.fromString(rawResponse); // 使用 Message 的静态 parse 方法

                    // 5. 校验 CorrelationID 是否匹配
                    if (correlationId.equals(responseMessage.getCorrelationId())) {
                        log.debug("CorrelationId matched. Processing response.");
                        connectionManager.returnConnection(socket);
                        // 6. 成功！反序列化业务内容并返回
                        return objectMapper.readValue(responseMessage.getContent(), responseType);
                    }
                    else {
                        log.warn("Received message with mismatched correlationId on reply topic. Expected: {}, Got: {}. Discarding.",
                                correlationId, responseMessage.getCorrelationId());
                        // 继续循环，等待正确的消息
                    }
                }

                // 短暂休眠，避免在没有消息时 CPU 100% 空转
                Thread.sleep(50);
            }

            // 7. 超时处理
            // 如果循环结束还没有返回，说明超时了
            connectionManager.invalidateConnection(socket); // 超时后连接状态未知，废弃掉
            throw new TimeoutException("No reply received for correlationId " + correlationId + " within " + timeoutMillis + "ms");

        } catch (Exception e) {
            if (socket != null) {
                connectionManager.invalidateConnection(socket);
            }
            // 将所有其他异常（网络、序列化等）封装为 RuntimeException 抛出
            throw new RuntimeException("Request-Reply operation failed", e);
        }
    }
}