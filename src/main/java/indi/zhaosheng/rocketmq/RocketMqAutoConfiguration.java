package indi.zhaosheng.rocketmq;

import indi.zhaosheng.rocketmq.annotation.RocketMqConsumer;
import indi.zhaosheng.rocketmq.consumer.DefaultRocketMqConsumer;
import indi.zhaosheng.rocketmq.consumer.IRocketMqMsgExecutor;
import indi.zhaosheng.rocketmq.producer.RocketMqProducer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author muluo
 * @description
 * @date 2020/5/12 20:20
 */
@Configuration
@EnableConfigurationProperties(RocketMqProperties.class)
@ConditionalOnClass(MQClientAPIImpl.class)
public class RocketMqAutoConfiguration {

    @Bean
    @ConditionalOnClass(DefaultMQProducer.class)
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = {"namesrvAddr", "producer.group"})
    public DefaultMQProducer mqProducer(RocketMqProperties rocketMqProperties) {

        RocketMqProperties.Producer producerConfig = rocketMqProperties.getProducer();
        String groupName = producerConfig.getGroup();
        Assert.hasText(groupName, "[spring.rocketmq.producer.group] must not be null");

        DefaultMQProducer producer = new DefaultMQProducer(producerConfig.getGroup());
        producer.setNamesrvAddr(rocketMqProperties.getNamesrvAddr());
        // producer.setSendMsgTimeout(producerConfig.getSendMsgTimeout());
        // producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        // producer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(1024 * 1024 * 4);
        // producer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMsgBodyOverHowmuch());
        // producer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryAnotherBrokerWhenNotStoreOk());

        return producer;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(name = "rocketMqProducer")
    public RocketMqProducer rocketMqProducer(DefaultMQProducer mqProducer) {
        RocketMqProducer rocketMqProducer = new RocketMqProducer();
        rocketMqProducer.setProducer(mqProducer);
        return rocketMqProducer;
    }

    @Configuration
    @ConditionalOnClass(DefaultMQPushConsumer.class)
    @EnableConfigurationProperties(RocketMqProperties.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = "namesrvAddr")
    public static class ConsumerConfiguration implements ApplicationContextAware, InitializingBean {

        private ConfigurableApplicationContext applicationContext;

        private AtomicLong counter = new AtomicLong(0);

        @Resource
        private StandardEnvironment environment;

        @Resource
        private RocketMqProperties rocketMqProperties;

        @Override
        public void afterPropertiesSet() {
            Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMqConsumer.class);
            beans.forEach(this::registerConsumers);
        }

        private void registerConsumers(String name, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);

            if (!IRocketMqMsgExecutor.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + IRocketMqMsgExecutor.class.getName());
            }

            IRocketMqMsgExecutor rocketMqMsgExecutor = (IRocketMqMsgExecutor) bean;
            RocketMqConsumer annotation = clazz.getAnnotation(RocketMqConsumer.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultRocketMqConsumer.class);
            beanBuilder.addPropertyValue("namesrvAddr", rocketMqProperties.getNamesrvAddr());
            beanBuilder.addPropertyValue("topic", environment.resolvePlaceholders(annotation.topic()));

            beanBuilder.addPropertyValue("consumerGroup", environment.resolvePlaceholders(annotation.consumerGroup()));
            beanBuilder.addPropertyValue("consumeModel", annotation.consumeModel());
            // beanBuilder.addPropertyValue(PROP_CONSUME_THREAD_MAX, annotation.consumeThreadMax());
            beanBuilder.addPropertyValue("messageModel", annotation.messageModel());
            // beanBuilder.addPropertyValue(PROP_SELECTOR_EXPRESS, environment.resolvePlaceholders(annotation.selectorExpress()));
            // beanBuilder.addPropertyValue(PROP_SELECTOR_TYPE, annotation.selectorType());
            beanBuilder.addPropertyValue("rocketMqMsgExecutor", rocketMqMsgExecutor);
            beanBuilder.setDestroyMethodName("destroy");

            String containerBeanName = String.format("%s_%s", DefaultRocketMqConsumer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultRocketMqConsumer container = beanFactory.getBean(containerBeanName, DefaultRocketMqConsumer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    // log.error("started container failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }
    }
}