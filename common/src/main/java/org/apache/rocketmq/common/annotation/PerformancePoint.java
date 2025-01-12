package org.apache.rocketmq.common.annotation;


import java.lang.annotation.*;

/**
 * 标注常见可能出现性能点的场景
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})
public @interface PerformancePoint {

    String value() default "";
}
