package io.github.jameswang777.minimq.consumer;

import io.github.jameswang777.minimq.config.MiniMqProperties;
import io.github.jameswang777.minimq.connection.ConnectionManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MiniMqListenerAnnotationBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware, ApplicationContextAware {

    private ApplicationContext applicationContext;

    // Dependencies will be lazily initialized later
    private ConnectionManager connectionManager;
    private ObjectMapper objectMapper;
    private MiniMqProperties properties;

    private final List<MiniMqListenerContainer> containers = new ArrayList<>();

    private ConfigurableBeanFactory beanFactory;

    public MiniMqListenerAnnotationBeanPostProcessor() {
        log.info("MiniMqListenerAnnotationBeanPostProcessor created. Dependencies will be resolved later.");
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // Spring will call this method automatically, giving us access to the context.
        this.applicationContext = applicationContext;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ConfigurableBeanFactory) {
            this.beanFactory = (ConfigurableBeanFactory) beanFactory;
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = bean.getClass();
        ReflectionUtils.doWithMethods(targetClass, method -> {
            MiniMqListener annotation = AnnotationUtils.findAnnotation(method, MiniMqListener.class);
            if (annotation != null) {
                // Found a listener method! Now it's time to get our dependencies.
                initDependenciesIfNecessary();

                // We only process beans if the consumer is enabled in properties
                if (!properties.getConsumer().isEnabled()) {
                    return;
                }

                // Found a listener method, create and start a container for it
                String rawTopic = annotation.topic();
                String resolvedTopic = this.beanFactory.resolveEmbeddedValue(rawTopic);

                // 如果注解中的 topic 解析后为空，则使用全局配置的 topic
                if (!StringUtils.hasText(resolvedTopic)) {
                    resolvedTopic = properties.getConsumer().getTopic();
                    log.debug("Listener on method [{}] has no specific topic, falling back to default topic: {}", method.getName(), resolvedTopic);
                } else {
                    log.info("Found listener on method [{}]. Raw topic: '{}', Resolved topic: '{}'", method.getName(), rawTopic, resolvedTopic);
                }

                processListenerMethod(bean, method, resolvedTopic);
            }
        }, ReflectionUtils.USER_DECLARED_METHODS);

        return bean;
    }

    /**
     * Lazily initializes dependencies from the ApplicationContext on first use.
     * This prevents the chicken-and-egg problem during startup.
     */
    private void initDependenciesIfNecessary() {
        if (this.connectionManager == null) {
            this.connectionManager = this.applicationContext.getBean(ConnectionManager.class);
            this.objectMapper = this.applicationContext.getBean(ObjectMapper.class);
            this.properties = this.applicationContext.getBean(MiniMqProperties.class);
        }
    }

    private void processListenerMethod(Object bean, Method method, String resolvedTopic) {
        validateListenerMethod(method);

        MiniMqListenerContainer container = new MiniMqListenerContainer(connectionManager, objectMapper, bean, method, resolvedTopic);
        containers.add(container);
        container.start();
    }

    private void validateListenerMethod(Method method) {
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
                // [MODIFIED] This is a header parameter, validate it against all known headers
                String headerName = headerAnnotation.value();
                boolean isKnownHeader = MiniMqHeaders.MESSAGE_ID.equals(headerName) ||
                        MiniMqHeaders.REPLY_TO.equals(headerName) ||
                        MiniMqHeaders.CORRELATION_ID.equals(headerName);

                if (!isKnownHeader) {
                    throw new IllegalArgumentException("Unsupported header value '" + headerName + "' on method " + method.getName());
                }
                if (!String.class.equals(parameter.getType())) {
                    throw new IllegalArgumentException("Parameter annotated with @Header(\"" + headerName + "\") must be of type String in method " + method.getName());
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
