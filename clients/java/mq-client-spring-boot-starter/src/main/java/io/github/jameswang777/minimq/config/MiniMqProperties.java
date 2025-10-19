package io.github.jameswang777.minimq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "minimq")
public class MiniMqProperties {

    /**
     * Enable MiniMQ auto-configuration.
     */
    private boolean enabled = true;

    /**
     * Broker server host.
     */
    private String host = "localhost";

    /**
     * Broker server port.
     */
    private String port = "5677";

    /**
     * Connection timeout in milliseconds.
     */
    private int connectionTimeout = 5000; // 5 seconds

    /**
     * Producer specific configurations.
     */
    private Producer producer = new Producer();

    /**
     * Consumer specific configurations.
     */
    private Consumer consumer = new Consumer();

    /**
     * Connection pool configurations.
     */
    private Pool pool = new Pool();

    @Data
    public static class Producer {
        /**
         * Number of retries for a failed send operation.
         */
        private int retries = 3;

        /**
         * Delay between retries in milliseconds.
         */
        private long retryDelayMs = 1000; // 1 second
    }

    @Data
    public static class Consumer {
        /**
         * Enable the consumer functionality.
         * Listeners will only be activated if this is true.
         */
        private boolean enabled = true;

        private String topic;
    }

    @Data
    public static class Pool {
        /**
         * Maximum number of active connections in the pool.
         */
        private int maxTotal = 8;

        /**
         * Maximum number of idle connections in the pool.
         */
        private int maxIdle = 8;

        /**
         * Minimum number of idle connections to maintain.
         */
        private int minIdle = 0;

        /**
         * Maximum time to wait for a connection from the pool when it is exhausted.
         */
        private long maxWaitMillis = 5000; // 5 seconds
    }
}
