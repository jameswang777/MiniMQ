package io.github.jameswang777.minimq.consumer;

import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class MiniMqListenerAnnotationBeanPostProcessor implements BeanPostProcessor {

    private final ConnectionManager connectionManager;
    private final ObjectMapper objectMapper;
    private final MiniMqProperties properties;
    private final List<MiniMqListenerContainer> containers = new ArrayList<>();

    public MiniMqListenerAnnotationBeanPostProcessor(ConnectionManager connectionManager, ObjectMapper objectMapper, MiniMqProperties properties) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // We only process beans if the consumer is enabled in properties
        if (!properties.getConsumer().isEnabled()) {
            return bean;
        }

        Class<?> targetClass = bean.getClass();
        ReflectionUtils.doWithMethods(targetClass, method -> {
            MiniMqListener annotation = AnnotationUtils.findAnnotation(method, MiniMqListener.class);
            if (annotation != null) {
                // Found a listener method, create and start a container for it
                processListenerMethod(bean, method);
            }
        }, ReflectionUtils.USER_DECLARED_METHODS);

        return bean;
    }

    private void processListenerMethod(Object bean, Method method) {
        // Basic validation
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("Method " + method.getName() + " annotated with @MyMqListener must have exactly one parameter.");
        }

        MiniMqListenerContainer container = new MiniMqListenerContainer(connectionManager, objectMapper, bean, method);
        containers.add(container);
        container.start();
    }

    @PreDestroy
    public void shutdown() {
        containers.forEach(MiniMqListenerContainer::stop);
    }
}
