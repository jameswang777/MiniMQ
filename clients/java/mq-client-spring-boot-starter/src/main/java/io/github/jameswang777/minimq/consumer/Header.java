package io.github.jameswang777.minimq.consumer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.PARAMETER) // 这个注解只能用在方法的参数上
@Retention(RetentionPolicy.RUNTIME) // 注解在运行时可见，以便我们通过反射读取
public @interface Header {

    /**
     * The name of the header to bind to this parameter.
     */
    String value();
}