package io.github.jameswang777.minimq.consumer;

import lombok.Getter;

@Getter
public class MessageWrapper {
    private final String id;
    private final String topic;
    private final String content;
    public static final String MESSAGE_DELIMITER = ":::";

    private MessageWrapper(String id, String topic, String content) {
        this.id = id;
        this.topic = topic;
        this.content = content;
    }

    public static MessageWrapper fromString(String rawMessage) {
        String[] parts = rawMessage.split(MessageWrapper.MESSAGE_DELIMITER, 3);
        if (parts.length != 3) {
            // In a real scenario, you might want to throw an exception or return an empty Optional
            return null;
        }
        return new MessageWrapper(parts[0], parts[1], parts[2]);
    }
}