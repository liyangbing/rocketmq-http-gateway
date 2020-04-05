package org.apache.rocketmq.gateway.common.protocol;

import java.util.Date;


public abstract class MessageProduceEntity extends MessageEntity {

    public static final String HEADER_CHARSET = "Message-Charset";

    public static final String HEADER_ONE_WAY = "One-Way";

    public static final String HEADER_DELAY_TIME_LEVEL = "Delay-Time-Level";

    public static final String HEADER_ORDERLY_SHARDING_KEY = "Orderly-Sharding-Key";

    // 请求标识
    protected String requestId;
    // HTTP协议版本
    protected String protocolVersion;
    // 消息编码
    protected String messageCharset = "UTF-8";

    protected boolean oneWay = false;

    protected String delayTimeLevel;

    protected String orderlyShardingKey;

    protected MessageProduceEntity() {
        // nothing to do.
    }

    protected MessageProduceEntity(final String internalReqId, final String gatewayAddress) {
        this.internalReqId = internalReqId;
        this.gatewayAddress = gatewayAddress;
        this.time = new Date();
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getMessageCharset() {
        return messageCharset;
    }

    public void setMessageCharset(String messageCharset) {
        this.messageCharset = messageCharset;
    }

    public boolean isOneWay() {
        return oneWay;
    }

    public void setOneWay(boolean oneWay) {
        this.oneWay = oneWay;
    }

    public String getDelayTimeLevel() {
        return delayTimeLevel;
    }

    public void setDelayTimeLevel(String delayTimeLevel) {
        this.delayTimeLevel = delayTimeLevel;
    }

    public String getOrderlyShardingKey() {
        return orderlyShardingKey;
    }

    public void setOrderlyShardingKey(String orderlyShardingKey) {
        this.orderlyShardingKey = orderlyShardingKey;
    }
}
