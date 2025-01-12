package org.apache.rocketmq.common.annotation;

import java.lang.annotation.*;

/**
 * 标注常见可能重点处理的场景
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})
public @interface ImportantPoint {

    String value();
}
