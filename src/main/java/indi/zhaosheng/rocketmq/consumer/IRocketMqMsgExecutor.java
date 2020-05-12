package indi.zhaosheng.rocketmq.consumer;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:17
 */
public interface IRocketMqMsgExecutor {
    boolean onMessage(String msg);
}
