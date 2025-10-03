package io.github.jameswang777.minimq.example;

import io.github.jameswang777.minimq.consumer.Header;
import io.github.jameswang777.minimq.consumer.MiniMqHeaders;
import io.github.jameswang777.minimq.consumer.MiniMqListener;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {

    // --- 简单模式：只关心消息体 ---
     @MiniMqListener(topic = "ORDER_TOPIC")
    public void handleOrderMessage(OrderDto order) {
        System.out.println("--- [Simple Listener] Received a message! ---");
        System.out.println("Processing order ID: " + order.getOrderId());
        System.out.println("Product: " + order.getProduct());
        System.out.println("Quantity: " + order.getQuantity());
        System.out.println("------------------------------------");
        // Simulate a processing error to test re-delivery
        if (order.getQuantity() > 10) {
            throw new RuntimeException("Quantity too high! This message will be re-queued.");
        }
    }

    // --- 高级模式：同时获取消息体和 Message ID ---
    // 注意：我们可以监听同一个 topic，MiniMQ 会把消息随机发给其中一个
    @MiniMqListener(topic = "ORDER_TOPIC_ADVANCED")
    public void handleOrderWithId(OrderDto order, @Header(MiniMqHeaders.MESSAGE_ID) String messageId) {
        System.out.println("--- [Advanced Listener] Received a message! ---");
        System.out.println("Broker Message ID from @Header: " + messageId); // <-- 我们拿到了ID！
        System.out.println("Processing order ID: " + order.getOrderId());
        System.out.println("Product: " + order.getProduct());
        System.out.println("Quantity: " + order.getQuantity());
        System.out.println("------------------------------------");
    }
}