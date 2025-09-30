package io.github.jameswang777.minimq.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import io.github.jameswang777.minimq.producer.MiniMqTemplate;
import io.github.jameswang777.minimq.consumer.MiniMqListenerAnnotationBeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MiniMqTemplate.class) // Only activate if the main class is present
@ConditionalOnProperty(prefix = "minimq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(MiniMqProperties.class)
public class MiniMqAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean // Allow users to provide their own ConnectionManager bean
    public ConnectionManager connectionManager(MiniMqProperties properties) {
        return new ConnectionManager(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MiniMqTemplate miniMqTemplate(ConnectionManager connectionManager, ObjectMapper objectMapper, MiniMqProperties properties) {
        return new MiniMqTemplate(connectionManager, objectMapper, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MiniMqListenerAnnotationBeanPostProcessor miniMqListenerAnnotationBeanPostProcessor(
            ConnectionManager connectionManager,
            ObjectMapper objectMapper,
            MiniMqProperties properties) {
        return new MiniMqListenerAnnotationBeanPostProcessor(connectionManager, objectMapper, properties);
    }

    /**
     * Provides a default ObjectMapper bean if one is not already present.
     * This is good practice as many applications will already have one.
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
