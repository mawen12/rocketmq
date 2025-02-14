package org.apache.rocketmq.common.mawen;


import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 标注该注解，代表了因为某些场景导致出现消息出现丢失，进而无法处理后续业务场景
 * @author mawen
 */
@Target({ElementType.FIELD, ElementType.ANNOTATION_TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.TYPE})
public @interface MessageLost {

    /**
     * @return 导致重复丢失的原因
     */
    String reason();
}
