package io.github.jameswang777.minimq.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.UUID;

@Getter
public class Message implements Serializable {
    public static final String MESSAGE_SPLITTER = ":::";
    private final String id;
    private final String topic;
    private final String content;
    @Setter
    private long timestamp; // 用于ACK超时检查

    public Message(String topic, String content) {
        this.id = UUID.randomUUID().toString();
        this.topic = topic;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    private Message(String id, String topic, String content) {
        this.id = id;
        this.topic = topic;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    // 从字符串反序列化为Message对象
    public static Message fromString(String str) {
        String[] parts = str.split(MESSAGE_SPLITTER, 3);
        if (parts.length != 3) {
            return null;
        }
        return new Message(parts[0], parts[1], parts[2]);
    }

    @Override
    public String toString() {
        // 定义一个用于网络传输和日志记录的格式
        return id + MESSAGE_SPLITTER + topic + MESSAGE_SPLITTER + content;
    }
}
