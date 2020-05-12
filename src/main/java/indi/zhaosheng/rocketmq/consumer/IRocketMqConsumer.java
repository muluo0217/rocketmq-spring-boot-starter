package indi.zhaosheng.rocketmq.consumer;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:16
 */
public interface IRocketMqConsumer {

    void setupRocketMqMsgExecutor(IRocketMqMsgExecutor rocketMqMsgExecutor);
}
