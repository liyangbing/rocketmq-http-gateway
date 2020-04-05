package org.apache.rocketmq.gateway.common;


import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

public final class GatewayConfig {

    // HTTP监听端口
    private int listenPort = 8888;
    // Server地址
    private String gatewayAddress = MixAll.LOCALHOST;
    // selector线程数
    private int selectorThreads = 3;
    // 执行器线程数
    private int workerThreads = 8;
    // 发送线程池最大大小
    private int sendThreadPoolQueueCapacity = 10000;
    // channel最大空间时间(秒)
    private int channelMaxIdleTimeSeconds = 30;
    // HTTP体最大长度
    private int httpTransferMaxContentLength = 1048576;
    // producer 空闲关闭时间,默认1天
    private int producerMaxIdleTimeSeconds = 86400;
    // producer空闲检测时间时间间隔，默认1小时
    private int producerIdleCheckIntervalSeconds = 3600;
    // name server 地址
    private String nameServerAddress;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public String getGatewayAddress() {
        return gatewayAddress;
    }

    public void setGatewayAddress(String gatewayAddress) {
        if (StringUtils.isNotBlank(gatewayAddress)) {
            this.gatewayAddress = gatewayAddress;
        }
    }

    public int getSelectorThreads() {
        return selectorThreads;
    }

    public void setSelectorThreads(int selectorThreads) {
        this.selectorThreads = selectorThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }

    public int getChannelMaxIdleTimeSeconds() {
        return channelMaxIdleTimeSeconds;
    }

    public void setChannelMaxIdleTimeSeconds(int channelMaxIdleTimeSeconds) {
        this.channelMaxIdleTimeSeconds = channelMaxIdleTimeSeconds;
    }

    public int getHttpTransferMaxContentLength() {
        return httpTransferMaxContentLength;
    }

    public void setHttpTransferMaxContentLength(int httpTransferMaxContentLength) {
        this.httpTransferMaxContentLength = httpTransferMaxContentLength;
    }

    public int getProducerMaxIdleTimeSeconds() {
        return producerMaxIdleTimeSeconds;
    }

    public void setProducerMaxIdleTimeSeconds(int producerMaxIdleTimeSeconds) {
        this.producerMaxIdleTimeSeconds = producerMaxIdleTimeSeconds;
    }

    public int getProducerIdleCheckIntervalSeconds() {
        return producerIdleCheckIntervalSeconds;
    }

    public void setProducerIdleCheckIntervalSeconds(int producerIdleCheckIntervalSeconds) {
        this.producerIdleCheckIntervalSeconds = producerIdleCheckIntervalSeconds;
    }

    public String getNameServerAddress() {
        return nameServerAddress;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }
}
