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
import java.lang.reflect.Parameter;
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
        validateListenerMethod(method);

        MiniMqListenerContainer container = new MiniMqListenerContainer(connectionManager, objectMapper, bean, method);
        containers.add(container);
        container.start();
    }

    private void validateListenerMethod(Method method)  {
        Parameter[] parameters = method.getParameters();
        if (parameters.length == 0) {
            throw new IllegalArgumentException("Method " + method.getName() + " annotated with @MiniMqListener must have at least one parameter for the payload.");
        }

        int payloadCount = 0;
        for (Parameter parameter : parameters) {
            Header headerAnnotation = parameter.getAnnotation(Header.class);
            if (headerAnnotation == null) {
                // This is a payload parameter
                payloadCount++;
            } else {
                // This is a header parameter, validate it
                if (!MiniMqHeaders.MESSAGE_ID.equals(headerAnnotation.value())) {
                    throw new IllegalArgumentException("Unsupported header value '" + headerAnnotation.value() + "' on method " + method.getName());
                }
                if (!String.class.equals(parameter.getType())) {
                    throw new IllegalArgumentException("Parameter annotated with @Header(\"" + MiniMqHeaders.MESSAGE_ID + "\") must be of type String in method " + method.getName());
                }
            }
        }

        if (payloadCount == 0) {
            throw new IllegalArgumentException("Method " + method.getName() + " must have exactly one parameter without @Header annotation for the message payload.");
        }

        if (payloadCount > 1) {
            throw new IllegalArgumentException("Method " + method.getName() + " must have only one parameter without @Header annotation for the message payload. Found: " + payloadCount);
        }
    }

    @PreDestroy
    public void shutdown() {
        containers.forEach(MiniMqListenerContainer::stop);
    }
}
