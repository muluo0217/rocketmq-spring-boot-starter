package indi.zhaosheng.rocketmq.annotation;

import indi.zhaosheng.rocketmq.enums.ConsumeModel;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.annotation.*;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:14
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RocketMqConsumer {

    String consumerGroup();

    String topic();

    MessageModel messageModel() default MessageModel.CLUSTERING;

    ConsumeModel consumeModel() default ConsumeModel.CONCURRENTLY;

    int consumeThreadMax() default 64;

    int pullThresholdForTopic() default -1;

    int pullThresholdSizeForTopic() default -1;
}
