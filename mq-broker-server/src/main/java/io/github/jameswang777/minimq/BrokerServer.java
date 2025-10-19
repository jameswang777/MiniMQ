package io.github.jameswang777.minimq;

import io.github.jameswang777.minimq.model.Message;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BrokerServer {
    // --- 1. 修改端口定义 ---
    private static final int PLAIN_PORT = 5677; // 非加密端口
    private static final int TLS_PORT = 5678;   // 加密端口

    private static final String LOG_FILE_PATH = "minimq-broker.log";
    private static final long ACK_TIMEOUT_MS = 30000; // 30秒超时

    // In-memory message queues, keyed by topic
    private final ConcurrentHashMap<String, BlockingQueue<Message>> messageQueues = new ConcurrentHashMap<>();
    // Messages sent to consumers but not yet acknowledged, keyed by message ID
    private final ConcurrentHashMap<String, Message> unackedMessages = new ConcurrentHashMap<>();
    // Scheduled executor for background tasks like ACK timeout scanning
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // --- 2. 新增线程池用于管理两个监听器 ---
    private final ExecutorService listenerExecutor = Executors.newFixedThreadPool(2);

    public BrokerServer() {
        log.info("Initializing MiniMQ Broker Server...");
        // 启动时从日志恢复消息
        loadMessagesFromLog();
        // 启动一个后台线程，用于检查超时的ACK
        startAckTimeoutScanner();
    }

    public static void main(String[] args) {
        try {
            new BrokerServer().start();
        } catch (Exception e) {
            log.error("Failed to start Broker Server.", e);
        }
    }

    // --- 3. 重写 start() 方法以支持双端口 ---
    public void start() throws IOException {
        // 启动非加密端口监听器
        try {
            ServerSocket plainSocket = new ServerSocket(PLAIN_PORT);
            listenerExecutor.submit(() -> startListenerLoop(plainSocket, "Plain TCP"));
            log.info("Broker Server is running (Plain TCP) on port {}", PLAIN_PORT);
        } catch (IOException e) {
            log.error("Could not start plain listener on port {}", PLAIN_PORT, e);
            throw e; // 如果非加密端口启动失败，则服务器启动失败
        }

        // 检查环境变量，决定是否启动加密端口监听器
        String keyStorePath = System.getenv("KEYSTORE_PATH");
        String keyStorePassword = System.getenv("KEYSTORE_PASSWORD");

        if (keyStorePath != null && !keyStorePath.isEmpty() && keyStorePassword != null) {
            try {
                SSLContext sslContext = createSslContext(keyStorePath, keyStorePassword);
                SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
                ServerSocket tlsSocket = factory.createServerSocket(TLS_PORT);
                listenerExecutor.submit(() -> startListenerLoop(tlsSocket, "TLS Encrypted"));
                log.info("Broker Server is running (TLS Encrypted) on port {}", TLS_PORT);
            } catch (Exception e) {
                // 加密端口启动失败只记录错误，不影响非加密服务的运行
                log.error("Could not start TLS listener on port {}. The server will continue to run without encryption.", TLS_PORT, e);
            }
        } else {
            log.warn("KEYSTORE_PATH or KEYSTORE_PASSWORD not set. TLS listener will not be started.");
        }
    }

    /**
     * 4. 统一的监听循环，可以接受普通和SSL的ServerSocket
     */
    private void startListenerLoop(ServerSocket serverSocket, String type) {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                log.info("Accepted new {} client connection from {}", type, clientSocket.getRemoteSocketAddress());
                new Thread(new ClientHandler(clientSocket, this)).start();
            } catch (IOException e) {
                log.error("Error accepting new {} connection", type, e);
            }
        }
    }

    /**
     * 5. 创建并初始化SSLContext的辅助方法
     */
    private SSLContext createSslContext(String keyStorePath, String keyStorePassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(Files.newInputStream(Paths.get(keyStorePath)), keyStorePassword.toCharArray());

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext;
    }

    // 核心方法：生产消息
    public synchronized void produce(Message message) {
        log.info("PRODUCE request for topic [{}], message ID [{}]", message.getTopic(), message.getId());
        // 1. 持久化到日志
        logMessage(message);
        // 2. 放入内存队列
        messageQueues.computeIfAbsent(message.getTopic(), k -> new LinkedBlockingQueue<>()).offer(message);
    }

    // 核心方法：消费消息
    public Message consume(String topic) throws InterruptedException {
        BlockingQueue<Message> queue = messageQueues.get(topic);
        if (queue == null) {
            return null; // 或者可以阻塞等待，这里为简化返回null
        }
        Message message = queue.poll(1, TimeUnit.SECONDS); // Use poll to avoid blocking the handler thread indefinitely
        // 放入待确认集合
        if (message != null) {
            message.setTimestamp(System.currentTimeMillis());
            unackedMessages.put(message.getId(), message);
            log.info("CONSUME request: Dispatched message [{}] from topic [{}]", message.getId(), topic);
        }
        return message;
    }

    // 核心方法：确认消息
    public synchronized void acknowledge(String messageId) {
        if (unackedMessages.remove(messageId) != null) {
            // 从持久化日志中移除（简化实现：重写日志文件）
            removeMessageFromLog(messageId);
            log.info("ACK received for message [{}]", messageId);
        }
    }

    // --- 持久化相关 ---
    private synchronized void logMessage(Message message) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(LOG_FILE_PATH, true)))) {
            out.println(message.toString());
        } catch (IOException e) {
            log.error("FATAL: Could not write message [{}] to log file!", message.getId(), e);
        }
    }

    private synchronized void removeMessageFromLog(String messageId) {
        File inputFile = new File(LOG_FILE_PATH);
        File tempFile = new File(LOG_FILE_PATH + ".tmp");

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 如果当前行不是要删除的消息，则将其写入临时文件
                if (!line.startsWith(messageId + ":")) {
                    writer.write(line + System.lineSeparator());
                }
            }
        } catch (IOException e) {
            log.error("Error while rewriting log file to remove message [{}]", messageId, e);
            return;
        }

        // 删除原文件并重命名临时文件
        if (!inputFile.delete() || !tempFile.renameTo(inputFile)) {
            log.error("CRITICAL: Failed to replace log file after removing message [{}]", messageId);
        }
    }

    private void loadMessagesFromLog() {
        File logFile = new File(LOG_FILE_PATH);
        if (!logFile.exists()) {
            log.info("Log file not found. Starting with a clean state.");
            return;
        }

        AtomicInteger count = new AtomicInteger(0);
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            reader.lines().forEach(line -> {
                Message message = Message.fromString(line);
                if (message != null) {
                    messageQueues.computeIfAbsent(message.getTopic(), k -> new LinkedBlockingQueue<>()).offer(message);
                    count.incrementAndGet();
                }
            });
            log.info("Successfully loaded {} unacknowledged messages from log file.", count.get());
        } catch (IOException e) {
            log.error("Failed to load messages from log file", e);
        }
    }

    // --- ACK超时检查 ---
    private void startAckTimeoutScanner() {
        scheduler.scheduleAtFixedRate(() -> {
            log.trace("Running ACK timeout scan...");
            long now = System.currentTimeMillis();
            unackedMessages.forEach((messageId, message) -> {
                if (now - message.getTimestamp() > ACK_TIMEOUT_MS) {
                    log.warn("Message timeout for ID [{}]. Re-queuing...", messageId);
                    // 从待确认集合中移除
                    unackedMessages.remove(messageId);
                    // 重新放回队列头部，优先处理
                    messageQueues.computeIfAbsent(message.getTopic(), k -> new LinkedBlockingQueue<>()).offer(message);
                }
            });
        }, 5, 5, TimeUnit.SECONDS);
        log.info("ACK timeout scanner started. Will check every 5 seconds.");
    }

}
