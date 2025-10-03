package io.github.jameswang777.minimq.consumer;

public final class MiniMqHeaders {

    private MiniMqHeaders() {
        // Private constructor to prevent instantiation
    }

    /**
     * The header for the unique message identifier assigned by the broker.
     */
    public static final String MESSAGE_ID = "MINIMQ_MESSAGE_ID";
}