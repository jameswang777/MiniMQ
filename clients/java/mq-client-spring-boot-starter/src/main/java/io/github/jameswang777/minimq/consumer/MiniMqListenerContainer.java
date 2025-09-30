package io.github.jameswang777.minimq.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;
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
    private final MiniMqListener listenerAnnotation;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MiniMqListenerContainer(ConnectionManager connectionManager, ObjectMapper objectMapper, Object bean, Method method) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
        this.bean = bean;
        this.method = method;
        this.listenerAnnotation = method.getAnnotation(MiniMqListener.class);
        this.executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "MyMqListener-" + listenerAnnotation.topic()));
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting listener for topic [{}] on method [{}]", listenerAnnotation.topic(), method.getName());
            executorService.submit(this::runListenerLoop);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping listener for topic [{}]", listenerAnnotation.topic());
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
                    out.printf("CONSUME:%s%n", listenerAnnotation.topic());

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
                log.info("Listener for topic [{}] was interrupted. Shutting down.", listenerAnnotation.topic());
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error in listener loop for topic [{}]. Reconnecting in 5 seconds...", listenerAnnotation.topic(), e);
                sleepBeforeReconnect();
            } finally {
                // Ensure connection is always returned or invalidated
                connectionManager.invalidateConnection(socket);
            }
        }
    }

    private void processMessage(String rawMessage, PrintWriter out) {
        String[] parts = rawMessage.split(":::", 3);
        if (parts.length != 3) {
            log.warn("Received malformed message: {}", rawMessage);
            return;
        }
        String messageId = parts[0];
        String messageContent = parts[2];

        try {
            // Deserialize message content to the type expected by the listener method
            Class<?> parameterType = method.getParameterTypes()[0];
            Object payload = objectMapper.readValue(messageContent, parameterType);

            // Invoke the user's listener method
            method.invoke(bean, payload);

            // Send ACK
            out.printf("ACK:%s%n", messageId);
            log.trace("Successfully processed and ACKed message [{}]", messageId);
        } catch (Exception e) {
            log.error("Error processing message [{}]. It will be re-queued after timeout.", messageId, e);
            // We don't send ACK, so the message will be re-delivered after timeout
        }
    }

    private void sleepBeforeReconnect() {
        try {
            Thread.sleep(5000); // Wait 5 seconds before trying to reconnect
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}