package io.github.jameswang777.minimq.consumer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD) // This annotation can only be applied to methods
@Retention(RetentionPolicy.RUNTIME) // The annotation should be available at runtime for processing
public @interface MiniMqListener {
    /**
     * The topic to listen to.
     */
    String topic();

    /**
     * A unique identifier for this listener container.
     * If not specified, a default one will be generated.
     */
    String id() default "";
}
