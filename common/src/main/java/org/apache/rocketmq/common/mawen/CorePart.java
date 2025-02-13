package org.apache.rocketmq.common.mawen;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 核心部分，代表业务逻辑的核心部分
 */
@Target({ElementType.FIELD, ElementType.ANNOTATION_TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.TYPE})
public @interface CorePart {

    String value();

    Part part() default Part.BROKER;

    enum Part {
        BROKER,

        PRODUCER,

        CONSUMER,

        BROKER_CONSUMER,

        PRODUCER_BROKER,

        MESSAGE,

        TOPIC,
    }
}
