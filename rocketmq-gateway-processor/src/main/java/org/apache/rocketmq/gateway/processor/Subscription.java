package org.apache.rocketmq.gateway.processor;

import java.io.Serializable;


public final class Subscription implements Serializable {

    // 应用标识
    private String app;
    // 回调地址
    private String callback;
    // tag表达式
    private String tag;
    // Topic
    private String topic;

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public int hashCode() {
        return 31 * app.hashCode() + topic.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Subscription)) {
            return false;
        }

        Subscription that = (Subscription) obj;
        return app.equals(that.app) && topic.equals(that.topic);
    }
}
