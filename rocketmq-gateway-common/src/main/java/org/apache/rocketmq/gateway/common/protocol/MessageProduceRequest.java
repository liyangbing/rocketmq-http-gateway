package org.apache.rocketmq.gateway.common.protocol;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.UUID;


public final class MessageProduceRequest extends MessageProduceEntity {

    public static final String LOG_PREFIX = "Received request: ";

    // 消息体
    private MessageContent content;
    // 请求方法
    private String method;

    public MessageProduceRequest() {
        // nothing to do.
    }

    public MessageProduceRequest(final String gatewayAddress) {
        super(UUID.randomUUID().toString(), gatewayAddress);
    }

    public MessageContent getContent() {
        return content;
    }

    public void setContent(MessageContent content) {
        this.content = content;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    /**
     * 消息体
     */
    public static class MessageContent implements Serializable {
        // tag
        private String tag;
        // key
        private String key;
        // 消息体
        private String body;

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }


}
