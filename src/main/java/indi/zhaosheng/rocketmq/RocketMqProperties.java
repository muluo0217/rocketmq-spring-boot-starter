package indi.zhaosheng.rocketmq;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:20
 */
@Setter
@Getter
@ConfigurationProperties(prefix = "spring.rocketmq")
public class RocketMqProperties {

    private String namesrvAddr;

    private Producer producer;

    @Setter
    @Getter
    public static class Producer {

        private String group;
    }
}
