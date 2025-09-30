package io.github.jameswang777.minimq.pool;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * A factory for creating and managing Socket objects for the connection pool.
 * Extends BasePooledObjectFactory to get default implementations for most methods.
 */
@Slf4j
public class PooledSocketFactory extends BasePooledObjectFactory<Socket> {

    private final String host;
    private final int port;
    private final int connectionTimeout;

    public PooledSocketFactory(String host, int port, int connectionTimeout) {
        this.host = host;
        this.port = port;
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Creates a new Socket object. This is the core method for the factory.
     * This method is called by the pool when a new object is needed.
     */
    @Override
    public Socket create() throws IOException {
        log.debug("Creating a new socket connection to {}:{}", host, port);
        Socket socket = new Socket();
        socket.setKeepAlive(true); // Important for long-lived connections
        socket.setTcpNoDelay(true); // Often good for request/response protocols
        socket.connect(new InetSocketAddress(host, port), connectionTimeout);
        return socket;
    }

    /**
     * Wraps a Socket instance with a PooledObject. This is required by the factory.
     */
    @Override
    public PooledObject<Socket> wrap(Socket socket) {
        return new DefaultPooledObject<>(socket);
    }

    /**
     * Destroys a Socket object. This is called when an object is removed from the pool.
     * The signature MUST match the parent method exactly: destroyObject(PooledObject<Socket> p)
     */
    @Override
    public void destroyObject(PooledObject<Socket> p) throws Exception {
        if (p != null && p.getObject() != null && p.getObject().isConnected()) {
            log.debug("Destroying socket connection to {}:{}", host, port);
            p.getObject().close();
        }
        super.destroyObject(p);
    }

    /**
     * Validates a Socket object before it is borrowed from the pool.
     * The signature MUST match the parent method exactly: validateObject(PooledObject<Socket> p)
     */
    @Override
    public boolean validateObject(PooledObject<Socket> p) {
        Socket socket = p.getObject();
        if (socket == null || !socket.isConnected() || socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            log.trace("Validation failed for socket object.");
            return false;
        }
        log.trace("Validation successful for socket object.");
        return true;
    }

}
