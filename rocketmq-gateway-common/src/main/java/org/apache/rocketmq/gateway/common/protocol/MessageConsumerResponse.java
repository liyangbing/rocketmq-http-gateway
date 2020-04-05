package org.apache.rocketmq.gateway.common.protocol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.Date;

public class MessageConsumerResponse extends MessageConsumerEntity {

    public static final String LOG_PREFIX = "Consumer Server response: ";

    private Boolean success;

    public MessageConsumerResponse() {
        // nothing to do.
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }


    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
