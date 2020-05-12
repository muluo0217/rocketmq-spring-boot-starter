package indi.zhaosheng.rocketmq.consumer;

import indi.zhaosheng.rocketmq.enums.ConsumeModel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:17
 */
@Slf4j
@Setter
@Getter
public class DefaultRocketMqConsumer implements IRocketMqConsumer, InitializingBean, DisposableBean {
    private String namesrvAddr;

    private String topic;

    private String consumerGroup;

    private String tags;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    private String charset = "UTF-8";

    private ConsumeModel consumeModel = ConsumeModel.CONCURRENTLY;

    private boolean started;

    private IRocketMqMsgExecutor rocketMqMsgExecutor;

    private DefaultMQPushConsumer consumer;

    @Override
    public void setupRocketMqMsgExecutor(IRocketMqMsgExecutor rocketMqMsgExecutor) {
        this.rocketMqMsgExecutor = rocketMqMsgExecutor;
    }

    @Override
    public void destroy() {
        this.setStarted(false);
        if (null != consumer) {
            consumer.shutdown();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    public void start() throws MQClientException {
        if (this.isStarted()) {
            return;
        }
        initConsumer();
        consumer.start();
        this.setStarted(true);
    }

    private void initConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setMessageModel(messageModel);

        consumer.subscribe(topic, tags);


        switch (consumeModel) {
            case ORDERLY:
                consumer.setMessageListener(messageListenerOrderly);
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(messageListenerConcurrently);
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

    }

    private MessageListenerConcurrently messageListenerConcurrently = (list, context) -> {
        MessageExt messageExt = list.get(0);
        String msg = new String(messageExt.getBody());
        log.info("{} Receive message: topic={}, tag={}, messageId={}, body={}", Thread.currentThread().getName(), messageExt.getTopic(), messageExt.getTags(), messageExt.getMsgId(), msg);
        boolean result = rocketMqMsgExecutor.onMessage(msg);
        return result ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS : ConsumeConcurrentlyStatus.RECONSUME_LATER;
    };

    private MessageListenerOrderly messageListenerOrderly = (list, context) -> {
        MessageExt messageExt = list.get(0);
        String msg = new String(messageExt.getBody());
        log.info("{} Receive message: topic={}, tag={}, messageId={}, body={}", Thread.currentThread().getName(), messageExt.getTopic(), messageExt.getTags(), messageExt.getMsgId(), msg);
        boolean result = rocketMqMsgExecutor.onMessage(msg);
        return result ? ConsumeOrderlyStatus.SUCCESS : ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
    };

    public IRocketMqMsgExecutor getRocketMqMsgExecutor() {
        return rocketMqMsgExecutor;
    }

    public void setRocketMqMsgExecutor(IRocketMqMsgExecutor rocketMqMsgExecutor) {
        //do nothing
    }
}
