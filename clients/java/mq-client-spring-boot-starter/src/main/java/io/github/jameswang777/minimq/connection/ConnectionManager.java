package io.github.jameswang777.minimq.connection;

import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.pool.PooledSocketFactory;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.Socket;
import java.net.URI;
import java.time.Duration;

@Slf4j
public class ConnectionManager {

    private final GenericObjectPool<Socket> connectionPool;

    public ConnectionManager(MiniMqProperties properties) {
        log.info("Initializing MiniMQ ConnectionManager...");
        // --- START: Robust Host and Port Parsing Logic ---
        String finalHost;
        int finalPort;

        try {
            // 优先尝试将 'host' 属性作为一个完整的 URI 来解析
            URI brokerUri = new URI(properties.getHost());
            finalHost = brokerUri.getHost();
            finalPort = brokerUri.getPort();

            // 验证从 URI 中是否成功提取了 host 和 port
            if (finalHost == null || finalPort == -1) {
                throw new IllegalArgumentException("Host or Port not found in the provided URI: " + properties.getHost());
            }
            log.info("Successfully parsed broker URI. Host='{}', Port='{}'", finalHost, finalPort);

        } catch (Exception e) {
            log.warn("Could not parse 'host' property as a URI. Falling back to using separate 'host' and 'port' properties. Reason: {}", e.getMessage());
            // 如果解析 URI 失败，则回退到使用独立的 host 和 port 属性
            finalHost = properties.getHost();
            try {
                finalPort = Integer.parseInt(properties.getPort());
            } catch (NumberFormatException nfe) {
                log.error("The configured 'port' property is not a valid number: '{}'", properties.getPort());
                // 这是一个致命的配置错误，抛出异常以阻止应用启动
                throw new IllegalArgumentException("Invalid port number configured: " + properties.getPort(), nfe);
            }
        }
        // --- END: Robust Host and Port Parsing Logic ---

        MiniMqProperties.Pool poolProps = properties.getPool();

        GenericObjectPoolConfig<Socket> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(poolProps.getMaxTotal());
        poolConfig.setMaxIdle(poolProps.getMaxIdle());
        poolConfig.setMinIdle(poolProps.getMinIdle());
        poolConfig.setMaxWait(Duration.ofMillis(poolProps.getMaxWaitMillis()));
        poolConfig.setTestOnBorrow(true); // Validate connection before use
        poolConfig.setTestOnReturn(true); // Also validate when returning
        poolConfig.setTestWhileIdle(true); // Periodically check idle connections

        PooledSocketFactory factory = new PooledSocketFactory(
                finalHost,
                finalPort,
                properties.getConnectionTimeout()
        );

        this.connectionPool = new GenericObjectPool<>(factory, poolConfig);
        log.info("MiniMQ Connection Pool configured with maxTotal={}", poolProps.getMaxTotal());
    }

    /**
     * Borrows a socket connection from the pool.
     * The caller is responsible for returning it.
     * @return A connected Socket.
     * @throws Exception if a connection cannot be borrowed.
     */
    public Socket borrowConnection() throws Exception {
        log.debug("Borrowing a connection from the pool...");
        return connectionPool.borrowObject();
    }

    /**
     * Returns a socket connection to the pool.
     * @param socket The socket to return.
     */
    public void returnConnection(Socket socket) {
        if (socket != null) {
            try {
                log.debug("Returning connection to the pool.");
                connectionPool.returnObject(socket);
            } catch (Exception e) {
                log.warn("Failed to return socket to pool. Destroying it instead.", e);
                invalidateConnection(socket);
            }
        }
    }

    /**
     * Invalidates a faulty connection, ensuring it is removed from the pool.
     * @param socket The faulty socket.
     */
    public void invalidateConnection(Socket socket) {
        if (socket != null) {
            try {
                log.warn("Invalidating a faulty connection.");
                connectionPool.invalidateObject(socket);
            } catch (Exception e) {
                log.error("Error while invalidating socket.", e);
            }
        }
    }

    @PreDestroy
    public void close() {
        log.info("Closing MiniMQ ConnectionManager and connection pool.");
        if (connectionPool != null && !connectionPool.isClosed()) {
            connectionPool.close();
        }
    }
}