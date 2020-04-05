package org.apache.rocketmq.gateway.processor.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.listener.MessageListener;

class ConsumeSubscription {

    public static final String ALL_TAG = "*";

    private String topic;

    private String tag;

    private MessageListener listener;

    public ConsumeSubscription() {
        // nothing to do.
    }

    public ConsumeSubscription(String topic, String tag, MessageListener listener) {
        setTopic(topic);
        setTag(tag);
        setListener(listener);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("topic is null.");
        }

        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        if (StringUtils.isBlank(tag) || ALL_TAG.equals(tag.trim())) {
            this.tag = null;
        } else {
            this.tag = tag;
        }
    }

    public MessageListener getListener() {
        return listener;
    }

    public void setListener(MessageListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener is null.");
        }
        this.listener = listener;
    }

    @Override
    public int hashCode() {
        int hash = topic.hashCode();
        if (!StringUtils.isEmpty(tag)) {
            hash = 31 * hash + tag.hashCode();
        }
        hash = 31 * hash + listener.getClass().getName().hashCode();

        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ConsumeSubscription)) {
            return false;
        }

        ConsumeSubscription that = (ConsumeSubscription) obj;
        return topic.equals(that.topic)
                && ((tag == null && that.tag == null) || (tag != null && tag.equals(that.tag)))
                && (listener.getClass().getName().equals(that.listener.getClass().getName()));
    }
}
