package org.apache.rocketmq.common.mawen;


import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 标注该注解，代表了因为某些场景导致出现消息重复推送，进而消息被重复消息的点
 * @author mawen
 */
@Target({ElementType.FIELD, ElementType.ANNOTATION_TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD})
public @interface RepeatConsume {

    /**
     * @return 导致重复消息的原因
     */
    String reason();
}
