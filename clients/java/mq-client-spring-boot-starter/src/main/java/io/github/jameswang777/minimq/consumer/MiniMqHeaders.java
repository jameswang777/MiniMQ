package io.github.jameswang777.minimq.consumer;

public final class MiniMqHeaders {

    private MiniMqHeaders() {
        // Private constructor to prevent instantiation
    }

    /**
     * The header for the unique message identifier assigned by the broker.
     */
    public static final String MESSAGE_ID = "MINIMQ_MESSAGE_ID";

    /**
     * The header for the correlation identifier used in request-reply messaging.
     */
    public static final String CORRELATION_ID = "MINIMQ_CORRELATION_ID";

    /**
     * The header for the reply-to destination in request-reply messaging.
     */
    public static final String REPLY_TO = "MINIMQ_REPLY_TO";
}