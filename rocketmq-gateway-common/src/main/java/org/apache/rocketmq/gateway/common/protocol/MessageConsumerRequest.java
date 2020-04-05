package org.apache.rocketmq.gateway.common.protocol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.Date;


public class MessageConsumerRequest extends MessageConsumerEntity {

    public static final String LOG_PREFIX = "Consumer Received request: ";


    public MessageConsumerRequest() {
        // nothing to do.
    }


    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
