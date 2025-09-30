package io.github.jameswang777.minimq.example;

import io.github.jameswang777.minimq.consumer.MiniMqListener;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {

    @MiniMqListener(topic = "ORDER_TOPIC")
    public void handleOrderMessage(OrderDto order) {
        // The 'order' object is automatically deserialized from the JSON message!
        System.out.println("--- Listener received a message! ---");
        System.out.println("Processing order ID: " + order.getOrderId());
        System.out.println("Product: " + order.getProduct());
        System.out.println("Quantity: " + order.getQuantity());
        System.out.println("------------------------------------");

        // Simulate a processing error to test re-delivery
         if (order.getQuantity() > 10) {
             throw new RuntimeException("Quantity too high! This message will be re-queued.");
         }
    }
}