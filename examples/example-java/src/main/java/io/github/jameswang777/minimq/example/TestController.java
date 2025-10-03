package io.github.jameswang777.minimq.example;

import io.github.jameswang777.minimq.producer.MiniMqTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class TestController {

    private final MiniMqTemplate miniMqTemplate;

    public TestController(MiniMqTemplate miniMqTemplate) {
        this.miniMqTemplate = miniMqTemplate;
    }

    @GetMapping("/send")
    public String sendMessage(@RequestParam(defaultValue = "MacBookPro") String product) {
        String orderId = UUID.randomUUID().toString();
        OrderDto order = new OrderDto(orderId, product, 1);

        System.out.println("--- Sending message from Controller ---");
        String messageId = miniMqTemplate.send("ORDER_TOPIC", order);

        String response = String.format("Message sent for order %s. Broker assigned messageId: %s", orderId, messageId);
        System.out.println(response);

        return response;
    }
}