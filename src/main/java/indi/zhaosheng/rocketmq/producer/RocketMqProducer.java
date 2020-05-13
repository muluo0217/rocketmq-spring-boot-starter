package indi.zhaosheng.rocketmq.producer;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Objects;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:38
 */
@Slf4j
@Setter
@Getter
public class RocketMqProducer implements InitializingBean, DisposableBean {


    private DefaultMQProducer producer;

    private String charset = "UTF-8";

    private MessageQueueSelector messageQueueSelector = new SelectMessageQueueByHash();

    @Override
    public void destroy() throws Exception {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        producer.start();
    }

    public boolean sendMsg(String topic, String tag, Object msg) {
        Message message = new Message(topic, tag, JSON.toJSONBytes(msg));
        try {
            SendResult sendResult = producer.send(message);
            log.info("msg send to topic:{}, msgId:{}, status:{}",
                    topic, sendResult.getMsgId(), sendResult.getSendStatus());
            return true;
        } catch (Exception e) {
            log.error("msg send to topic:{}, body:{}, error context:{}",
                    topic, JSON.toJSONString(message), e);
        }
        return false;
    }
}