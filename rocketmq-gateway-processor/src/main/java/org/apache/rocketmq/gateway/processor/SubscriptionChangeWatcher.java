package org.apache.rocketmq.gateway.processor;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.gateway.processor.consumer.ConsumerManager;
import org.apache.rocketmq.gateway.processor.producer.ProducerManager;

import java.util.List;

// 动态监听，生产和消费订阅关系变化
public class SubscriptionChangeWatcher {

    private ProducerManager producerManager;
    private ConsumerManager consumerManager;

    private static final String PRODUCER_SUBSCRIPTION_DATA_DEMO = "[{\"app\":\"gateway_demo\", \"topic\":\"test\"}]";
    private static final String CONSUME_SUBSCRIPTION_DATA_DEMO = "[{\"app\":\"gateway_demo\",\"callback\":\"http://www.baidu.com/index.do\",\"tag\":\"\",\"topic\":\"test\"}]";

    public SubscriptionChangeWatcher(ProducerManager producerManager, ConsumerManager consumerManager) {
        this.producerManager = producerManager;
        this.consumerManager = consumerManager;
    }

    public void init() {
        onProducerChange();
        onConsumerChange();
    }

    void onProducerChange() {
        List<Subscription> subscriptions = JSON.parseArray(PRODUCER_SUBSCRIPTION_DATA_DEMO, Subscription.class);
        producerManager.onSubscription(subscriptions);
    }

    void onConsumerChange() {
        List<Subscription> subscriptions = JSON.parseArray(CONSUME_SUBSCRIPTION_DATA_DEMO, Subscription.class);
        consumerManager.onSubscription(subscriptions);
    }

}
