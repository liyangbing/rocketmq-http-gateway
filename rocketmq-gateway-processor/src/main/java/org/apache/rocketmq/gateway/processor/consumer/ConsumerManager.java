package org.apache.rocketmq.gateway.processor.consumer;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.gateway.common.GatewayConfig;
import org.apache.rocketmq.gateway.common.utils.Service;
import org.apache.rocketmq.gateway.processor.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;


public final class ConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

    private GatewayConfig gatewayConfig;
    private final CloseableHttpClient httpClient;

    private final ConcurrentHashMap<String /* app */, InternalConsumer> consumerMap = new ConcurrentHashMap<>();

    public ConsumerManager(final GatewayConfig gatewayConfig, final CloseableHttpClient httpClient) {
        this.gatewayConfig = gatewayConfig;
        this.httpClient = httpClient;
    }

    public void onSubscription(List<Subscription> subscriptions) {
        Map<String /* app */, List<Subscription>> appSubscriptions = subscriptions.stream()
                .collect(Collectors.groupingBy(Subscription::getApp));

        // 处理每个应用的订阅信息
        if (appSubscriptions == null || appSubscriptions.isEmpty()) {
            logger.info("No subscription for gateway.");

        } else {
            for (Map.Entry<String, List<Subscription>> entry : appSubscriptions.entrySet()) {
                try {
                    onSubscription(entry.getKey(), entry.getValue());
                } catch (MQClientException e) {
                    throw new RuntimeException(String.format("Create consumer or subscribe topic error, %s", entry.getKey()), e);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("Encode callback url with UTF-8 error.", e);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Start consumer %s error.", entry.getKey()), e);
                }
            }
        }

        // 停止取消订阅的应用
        Set<String> apps = (appSubscriptions != null && !appSubscriptions.isEmpty()) ? appSubscriptions.keySet() : null;
        Iterator<Map.Entry<String, InternalConsumer>> iterator = consumerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, InternalConsumer> entry = iterator.next();
            String app = entry.getKey();
            if (apps == null || !apps.contains(app)) {
                entry.getValue().stop();
                iterator.remove();

                logger.info("Stop consumer[{}] and remove it.", app);
            }
        }
    }

    private void onSubscription(String app, List<Subscription> newSubscriptions) throws Exception {
        InternalConsumer consumer = consumerMap.get(app);
        if (consumer == null) {
            consumer = new InternalConsumer(app);
            InternalConsumer prev = consumerMap.putIfAbsent(app, consumer);
            if (prev != null) {
                consumer = prev;
            }
        }

        // 修改订阅
        if (newSubscriptions != null) {
            Map<String, Subscription> newTopicSubscriptionMap = new HashMap<>(newSubscriptions.size());
            for(Subscription subscription : newSubscriptions) {
                newTopicSubscriptionMap.put(subscription.getTopic(), subscription);
            }
            for (Subscription oldSubscription : consumer.subscriptions) {
                Subscription newTopicSubscription = newTopicSubscriptionMap.get(oldSubscription.getTopic());
                // 内存和db中都有订阅关系
                if (newTopicSubscription != null) {
                    if (!oldSubscription.getTag().equals(newTopicSubscription.getTag())
                            || !oldSubscription.getCallback().equals(newTopicSubscription.getCallback())) {
                        consumer.unsubscribe(oldSubscription);
                        consumer.subscribe(newTopicSubscription);
                    }
                }
            }
        }

        // 新增订阅
        Collection<Subscription> adds = CollectionUtils.subtract(newSubscriptions, consumer.subscriptions);
        if (adds != null && !adds.isEmpty()) {
            for (Subscription add : adds) {
                consumer.subscribe(add);
            }
        }

        // 删除定阅
        Collection<Subscription> deletes = CollectionUtils.subtract(consumer.subscriptions, newSubscriptions);
        if (deletes != null && !deletes.isEmpty()) {
            for (Subscription subscription : deletes) {
                consumer.unsubscribe(subscription);
            }
        }

        if (!consumer.isStarted()) {
            consumer.start();
        }

        logger.info("Start consumer[{}] ok.", app);
    }

    void stop() {
        Iterator<Map.Entry<String, InternalConsumer>> iterator = consumerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, InternalConsumer> entry = iterator.next();
            entry.getValue().stop();
            iterator.remove();

            logger.info("Stop consumer[{}] ok.", entry.getKey());
        }
    }

    public class InternalConsumer extends Service {
        private final String app;
        private final DefaultMQPushConsumer consumer;

        private List<Subscription> subscriptions;
        private Map<String/*topic*/, ConsumeSubscription> subscriptionMap;

        InternalConsumer(final String app) {
            this.consumer = new DefaultMQPushConsumer(app);
            this.consumer.setNamesrvAddr(gatewayConfig.getNameServerAddress());
            this.app = app;

            this.subscriptions = new CopyOnWriteArrayList<>();
            this.subscriptionMap = new ConcurrentHashMap<>();
        }

        public void doStart() throws Exception {
            super.doStart();
            this.consumer.registerMessageListener(new DefaultMessageListenerConcurrently());
            consumer.start();
        }

        public void doStop() {
            consumer.shutdown();

            subscriptionMap = null;
            subscriptions = null;

            super.doStop();
        }

        void subscribe(Subscription subscription) throws MQClientException, UnsupportedEncodingException {
            String topic = subscription.getTopic();
            String tag = subscription.getTag();
            String callback = subscription.getCallback();
            if (StringUtils.isBlank(topic)) {
                throw new MQClientException("topic is null", null);
            }

            subscriptions.add(subscription);
            HttpMessageListenerConcurrently listener =
                    new HttpMessageListenerConcurrently(callback, topic, tag, app, httpClient, gatewayConfig.getGatewayAddress());
            ConsumeSubscription old = subscriptionMap.put(topic, new ConsumeSubscription(topic, tag, listener));
            if (old != null) {
                logger.warn(String.format("duplicated subscription with topic[%s], override it.", topic));
            }

            consumer.subscribe(topic, tag);
        }

        void unsubscribe(Subscription subscription) {
            String topic = subscription.getTopic();
            if (StringUtils.isBlank(topic)) {
                return;
            }
            consumer.unsubscribe(topic);

            subscriptions.remove(subscription);
            subscriptionMap.remove(topic);
        }

        private class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                if (messages == null || messages.isEmpty()) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

                Map<String/*topic*/, List<MessageExt>> data = messages.stream().collect(Collectors.groupingBy(MessageExt::getTopic));

                RuntimeException ex = null;
                for (Map.Entry<String, List<MessageExt>> messageEntry : data.entrySet()) {
                    String topic = messageEntry.getKey();
                    ConsumeSubscription consumeSubscription = subscriptionMap.get(topic);
                    if (consumeSubscription == null || consumeSubscription.getListener() == null) {
                        ex = new RuntimeException(String.format("no message listener for topic[%s] app[%s]", topic, consumer.getConsumerGroup()));
                        logger.error(ex.getMessage(), ex);
                        continue;
                    }

                    try {
                        ((HttpMessageListenerConcurrently) consumeSubscription.getListener()).consumeMessage(messageEntry.getValue(), context);
                    } catch (Exception e) {

                        ex = new RuntimeException(String.format("topic[%s] app[%s] consume message with listener error."
                                + e.getMessage(), topic, consumer.getConsumerGroup()), e);
                        logger.error(ex.getMessage(), ex);
                    }
                }

                if (ex != null) {
                    throw ex;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        }
    }

    public void setGatewayConfig(GatewayConfig gatewayConfig) {
        this.gatewayConfig = gatewayConfig;
    }
}
