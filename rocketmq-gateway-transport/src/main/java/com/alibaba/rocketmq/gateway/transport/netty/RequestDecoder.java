package com.alibaba.rocketmq.gateway.transport.netty;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest.MessageContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

import static org.apache.rocketmq.gateway.common.protocol.MessageEntity.*;
import static org.apache.rocketmq.gateway.common.protocol.MessageProduceEntity.*;


@Sharable
final class RequestDecoder extends MessageToMessageDecoder<FullHttpRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RequestDecoder.class);

    private final String gatewayAddress;

    RequestDecoder(final String gatewayAddress) {
        this.gatewayAddress = gatewayAddress;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        MessageProduceRequest request = new MessageProduceRequest(gatewayAddress);
        HttpHeaders headers = msg.headers();

        final String topic = headers.get(HEADER_TOPIC);
        if (StringUtils.isNotBlank(topic)) {
            request.setTopic(topic.trim());
        }

        final String app = headers.get(HEADER_APP);
        if (StringUtils.isNotBlank(app)) {
            request.setApp(app.trim());
        }

        final String requestId = headers.get(HEADER_REQ_ID);
        if (StringUtils.isNotBlank(requestId)) {
            request.setRequestId(requestId.trim());
        }

        final String charset = headers.get(HEADER_CHARSET);
        if (StringUtils.isNotBlank(charset)) {
            request.setMessageCharset(charset.trim());
        }

        if (headers.contains(HEADER_ONE_WAY)) {
            request.setOneWay(true);
        }

        final String delayTimeLevel = headers.get(HEADER_DELAY_TIME_LEVEL);
        if (StringUtils.isNotBlank(delayTimeLevel)) {
            request.setDelayTimeLevel(delayTimeLevel);
        }

        final String orderlyShardingKey = headers.get(HEADER_ORDERLY_SHARDING_KEY);
        if (StringUtils.isNotBlank(orderlyShardingKey)) {
            request.setOrderlyShardingKey(orderlyShardingKey);
        }

        final String method = msg.getMethod().name();
        request.setMethod(method);
        request.setProtocolVersion(msg.getProtocolVersion().text());

        try {
            ByteBuf buffer = msg.content();
            String content = buffer != null ? buffer.toString(Charset.forName(request.getMessageCharset())) : null;
            if (StringUtils.isNotBlank(content)) {
                request.setContent(JSON.parseObject(content.trim(), MessageContent.class));
            }

        } catch (Exception e) {
            logger.error(String.format("decode request error, %s", msg), e);
        }

        out.add(request);

    }

}
