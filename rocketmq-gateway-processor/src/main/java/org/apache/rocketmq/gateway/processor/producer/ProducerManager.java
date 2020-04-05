package org.apache.rocketmq.gateway.processor.producer;


import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.gateway.common.GatewayConfig;
import org.apache.rocketmq.gateway.processor.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


public final class ProducerManager {

    private static final Logger logger = LoggerFactory.getLogger(ProducerManager.class);

    private static HashMap<String, ProducerWrapper> appProducerMap = new HashMap<>();

    public static final String APP_TOPIC_SUBSCRIPTION_SEPARATOR = ":";

    /**
     * app code  和topic code 映射关系
     * key appCode + : + topicCode
     */
    private volatile Set<String> appTopicSubscriptionSet = new HashSet<>();

    private GatewayConfig gatewayConfig;

    // 空闲生产者检测
    private IdleProducerMonitorThread idleMonitor;

    public ProducerManager() {
        idleMonitor = new IdleProducerMonitorThread("idle-producer-monitor");
        idleMonitor.setDaemon(true);
    }

    public DefaultMQProducer getProducer(String app) throws Exception {
        ReentrantLock lock = getLock(app);

        lock.lock();
        try {
            ProducerWrapper wrapper = appProducerMap.get(app);
            if (wrapper == null) {
                DefaultMQProducer defaultMQProducer = new DefaultMQProducer(app);
                defaultMQProducer.setNamesrvAddr(gatewayConfig.getNameServerAddress());

                wrapper = new ProducerWrapper(defaultMQProducer);
                wrapper.producer.start();

                appProducerMap.put(app, wrapper);
            }

            wrapper.lastActiveNanoTime = System.nanoTime();
            return wrapper.producer;

        } finally {
            lock.unlock();
        }
    }

    public void start() {
        idleMonitor.start();
    }

    public void stop() {
        idleMonitor.shutdown();
        shutdownAllProducers();
    }

    /**
     * 关闭所有的生产者
     */
    private void shutdownAllProducers() {
        shutdownProducers0(true);
    }

    /**
     * 关闭空闲生产者
     */
    private void shutdownIdleProducers() {
        shutdownProducers0(false);
    }

    private void shutdownProducers0(boolean isShutdown) {
        String app;
        ProducerWrapper wrapper;
        MQProducer producer;
        long idleTime;

        boolean needClose;
        ReentrantLock lock;
        Map.Entry<String, ProducerWrapper> entry;
        Iterator<Map.Entry<String, ProducerWrapper>> iterator = appProducerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            entry = iterator.next();
            app = entry.getKey();
            wrapper = entry.getValue();

            idleTime = 0;

            lock = getLock(app);
            lock.lock();
            try {
                if (!isShutdown) {
                    // 非shutdown,则只关闭空闲生产者
                    idleTime = (System.nanoTime() - wrapper.lastActiveNanoTime) / 1000 / 1000 / 1000;
                    needClose = idleTime > gatewayConfig.getProducerMaxIdleTimeSeconds();
                } else {
                    // shutdown,关闭所有生产者
                    needClose = true;
                }

                if (needClose) {
                    iterator.remove();

                    producer = wrapper.producer;
                    producer.shutdown();

                    if (isShutdown) {
                        logger.info(String.format("gateway shutdown, close producer[%s]", app));
                    } else {
                        logger.info(String.format("producer[%s]'s idle time[%ds] exceeds the max idle time[%ds], close it.",
                                app, idleTime, gatewayConfig.getProducerMaxIdleTimeSeconds()));
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private static ConcurrentHashMap<String /*应用编码*/, ReentrantLock> appMutexMap = new ConcurrentHashMap<>();

    private ReentrantLock getLock(final String app) {
        ReentrantLock lock = appMutexMap.get(app);
        if (lock == null) {
            lock = new ReentrantLock();
            ReentrantLock prev = appMutexMap.putIfAbsent(app, lock);
            if (prev != null) {
                lock = prev;
            }
        }

        return lock;
    }

    /**
     * 空闲生产者检测
     */
    private class IdleProducerMonitorThread extends Thread {

        private volatile boolean shutdown;

        IdleProducerMonitorThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(gatewayConfig.getProducerIdleCheckIntervalSeconds() * 1000);
                        shutdownIdleProducers();
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }

        void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public void setGatewayConfig(GatewayConfig gatewayConfig) {
        this.gatewayConfig = gatewayConfig;
    }

    public Set<String> getAppTopicSubscriptionSet() {
        return appTopicSubscriptionSet;
    }

    public void onSubscription(List<Subscription> subscriptions) {
        Set<String> appTopicSubscriptionLocal;
        if (subscriptions == null || subscriptions.isEmpty()) {
            logger.warn("No subscription.");
            appTopicSubscriptionLocal = new HashSet<>(0);
        } else {
            appTopicSubscriptionLocal = new HashSet<>(subscriptions.size());
            for (Subscription subscription : subscriptions) {
                if (StringUtils.isNotBlank(subscription.getApp())
                        && StringUtils.isNotBlank(subscription.getTopic())) {
                    appTopicSubscriptionLocal.add(subscription.getApp() + APP_TOPIC_SUBSCRIPTION_SEPARATOR + subscription.getTopic());
                }
            }
        }

        appTopicSubscriptionSet = appTopicSubscriptionLocal;
    }

    /**
     * 生产者包装器
     */
    private class ProducerWrapper {

        // 生产者
        private final DefaultMQProducer producer;

        // 最新活跃时间(纳秒)
        private long lastActiveNanoTime;

        ProducerWrapper(final DefaultMQProducer producer) {
            this.producer = producer;
            this.lastActiveNanoTime = System.nanoTime();
        }

    }

}
