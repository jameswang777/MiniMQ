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

    private final String correlationId; // 用于匹配请求和响应
    private final String replyTo;       // 指定响应应该发送到哪个主题

    @Setter
    private long timestamp; // 用于ACK超时检查

    // 构造器给异步消息使用
    public Message(String topic, String content) {
        this(topic, content, null, null);
    }

    // 构造器给同步请求消息使用
    public Message(String topic, String content, String correlationId, String replyTo) {
        this.id = UUID.randomUUID().toString();
        this.topic = topic;
        this.content = content;
        this.correlationId = correlationId;
        this.replyTo = replyTo;
        this.timestamp = System.currentTimeMillis();
    }

    // 私有构造器，用于反序列化
    private Message(String id, String topic, String content, String correlationId, String replyTo) {
        this.id = id;
        this.topic = topic;
        this.content = content;
        this.correlationId = correlationId;
        this.replyTo = replyTo;
        this.timestamp = System.currentTimeMillis();
    }

    // 从字符串反序列化为Message对象
    public static Message fromString(String str) {
        // 使用 limit -1 来保留末尾的空字符串，以兼容旧格式
        String[] parts = str.split(MESSAGE_SPLITTER, 5);
        if (parts.length < 3) {
            return null;
        }
        String id = parts[0];
        String topic = parts[1];
        String content = parts[2];
        // 向后兼容：如果旧客户端只发送3个部分，correlationId和replyTo为null
        String correlationId = (parts.length > 3) ? parts[3] : null;
        String replyTo = (parts.length > 4) ? parts[4] : null;

        // 处理空字符串 "" 被解析为 null
        if (correlationId != null && correlationId.isEmpty()) correlationId = null;
        if (replyTo != null && replyTo.isEmpty()) replyTo = null;

        return new Message(id, topic, content, correlationId, replyTo);
    }

    @Override
    public String toString() {
        // 升级协议：增加 correlationId 和 replyTo
        // 使用空字符串""表示null，避免传输"null"字符串
        return id + MESSAGE_SPLITTER +
                topic + MESSAGE_SPLITTER +
                content + MESSAGE_SPLITTER +
                (correlationId == null ? "" : correlationId) + MESSAGE_SPLITTER +
                (replyTo == null ? "" : replyTo);
    }
}
