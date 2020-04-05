package org.apache.rocketmq.gateway.common.protocol;

import java.io.Serializable;
import java.util.Date;

public abstract class MessageEntity implements Serializable {

    public static final String HEADER_TOPIC = "Topic";

    public static final String HEADER_APP = "App";

    public static final String HEADER_REQ_ID = "Request-ID";

    // topic
    protected String topic;
    // 应用编码
    protected String app;
    // 内部请求标识
    protected String internalReqId;
    // 网关IP
    protected String gatewayAddress;
    // 接收请求或者返回应答的时间
    protected Date time;

    protected MessageEntity() {
        // nothing to do.
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getInternalReqId() {
        return internalReqId;
    }

    public void setInternalReqId(String internalReqId) {
        this.internalReqId = internalReqId;
    }

    public String getGatewayAddress() {
        return gatewayAddress;
    }

    public void setGatewayAddress(String gatewayAddress) {
        this.gatewayAddress = gatewayAddress;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public abstract String toString();

}
