package org.apache.rocketmq.gateway.common.protocol;


public abstract class MessageConsumerEntity extends MessageEntity {

    public static final String HEADER_TAG = "Tag";
    public static final String HEADER_KEY = "Key";

    public static final String LOG_PREFIX = "Consumer Received request: ";

    // tag
    protected String tag;
    // 回调地址
    protected String callback;
    // 消息key
    protected String key;
    // 请求或应答内容
    protected String content;

    protected MessageConsumerEntity() {
        // nothing to do.
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageConsumerEntity that = (MessageConsumerEntity) o;
        return topic.equals(that.topic) && app.equals(that.app);

    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + app.hashCode();
        return result;
    }
}
