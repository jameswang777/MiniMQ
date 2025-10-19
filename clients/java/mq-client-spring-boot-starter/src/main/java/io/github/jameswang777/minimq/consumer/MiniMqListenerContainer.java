package io.github.jameswang777.minimq.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import io.github.jameswang777.minimq.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MiniMqListenerContainer {

    private final ConnectionManager connectionManager;
    private final ObjectMapper objectMapper;
    private final Object bean;
    private final Method method;
    private final String topic;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MiniMqListenerContainer(ConnectionManager connectionManager, ObjectMapper objectMapper, Object bean, Method method, String topic) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
        this.bean = bean;
        this.method = method;
        this.topic = topic;
        this.executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "MiniMqListener-" + this.topic));
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting listener for topic [{}] on method [{}]", this.topic, method.getName());
            executorService.submit(this::runListenerLoop);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping listener for topic [{}]", this.topic);
            executorService.shutdownNow(); // Interrupt the listening thread
        }
    }

    private void runListenerLoop() {
        while (running.get()) {
            Socket socket = null;
            try {
                socket = connectionManager.borrowConnection();
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Main loop for this connection
                while (running.get() && socket.isConnected()) {
                    // 1. Send CONSUME request
                    out.printf("CONSUME:%s%n", this.topic);

                    // 2. Wait for response
                    String response = in.readLine();
                    if (response == null) { // Connection closed by broker
                        break;
                    }

                    if ("NO_MSG".equals(response)) {
                        Thread.sleep(1000); // Wait before polling again
                        continue;
                    }

                    // 3. Process the message
                    processMessage(response, out);
                }
            } catch (InterruptedException e) {
                log.info("Listener for topic [{}] was interrupted. Shutting down.", this.topic);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error in listener loop for topic [{}]. Reconnecting in 5 seconds...", this.topic, e);
                sleepBeforeReconnect();
            } finally {
                // Ensure connection is always returned or invalidated
                connectionManager.invalidateConnection(socket);
            }
        }
    }

    private void processMessage(String rawMessage, PrintWriter out) {
        Message message = Message.fromString(rawMessage);
        if (message == null) {
            log.warn("Received malformed message: {}", rawMessage);
            return;
        }
        try {
            // 1. 准备参数列表
            Object[] args = prepareArguments(message);

            // 2. 使用准备好的参数调用用户方法
            method.invoke(bean, args);

            // 3. 发送 ACK
            out.printf("ACK:%s%n", message.getId());
            log.trace("Successfully processed and ACKed message [{}]", message.getId());
        } catch (Exception e) {
            log.error("Error processing message [{}]. It will be re-queued after timeout.", message.getId(), e);
            // No ACK is sent on failure
        }
    }

    private Object[] prepareArguments(Message message) throws Exception {
        Parameter[] parameters = method.getParameters(); // 使用 getParameters() 更现代
        Object[] args = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            Header headerAnnotation = parameter.getAnnotation(Header.class);

            if (headerAnnotation != null) {
                // --- This is a header parameter, use a switch for clarity ---
                String headerName = headerAnnotation.value();

                // The parameter type was already validated to be String in the PostProcessor
                switch (headerName) {
                    case MiniMqHeaders.MESSAGE_ID:
                        args[i] = message.getId();
                        break;
                    case MiniMqHeaders.REPLY_TO:
                        args[i] = message.getReplyTo(); // Will be null if not present
                        break;
                    case MiniMqHeaders.CORRELATION_ID:
                        args[i] = message.getCorrelationId(); // Will be null if not present
                        break;
                    default:
                        // This case should technically not be reached due to validation
                        log.warn("Unsupported header '{}' requested by method {}. Passing null.", headerName, method.getName());
                        args[i] = null;
                        break;
                }
            } else {
                // --- 这是一个没有注解的参数，我们假定它是消息体 (payload) ---
                // (为了健壮性，我们应该确保只有一个 payload 参数，这在 BeanPostProcessor 中验证)
                try {
                    args[i] = objectMapper.readValue(message.getContent(), parameter.getType());
                } catch (Exception e) {
                    log.error("Failed to deserialize payload for method {}", method.getName(), e);
                    throw e; // 重新抛出，让上层捕获并处理
                }
            }
        }

        return args;
    }

    private void sleepBeforeReconnect() {
        try {
            Thread.sleep(5000); // Wait 5 seconds before trying to reconnect
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}