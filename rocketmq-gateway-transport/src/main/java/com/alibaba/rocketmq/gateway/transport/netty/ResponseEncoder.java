package com.alibaba.rocketmq.gateway.transport.netty;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.gateway.transport.util.TransportUtils;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.rocketmq.gateway.common.protocol.MessageEntity.*;


@Sharable
final class ResponseEncoder extends MessageToMessageEncoder<MessageProduceResponse> {

    private static final Logger logger = LoggerFactory.getLogger(ResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, MessageProduceResponse msg, List<Object> out) throws Exception {
        HttpVersion version = HttpVersion.HTTP_1_0;
        if (HttpVersion.HTTP_1_1.text().equals(msg.getProtocolVersion())) {
            version = HttpVersion.HTTP_1_1;
        }

        final String topic = StringUtils.isEmpty(msg.getTopic()) ? "" : msg.getTopic();
        final String app = StringUtils.isEmpty(msg.getApp()) ? "" : msg.getApp();
        final String requestId = StringUtils.isEmpty(msg.getRequestId()) ? "" : msg.getRequestId();

        try {
            final String content = JSON.toJSONString(new ResponseBody(msg.getStatus(), msg.getMsgId()));

            FullHttpResponse response = new DefaultFullHttpResponse(version, HttpResponseStatus.OK);
            response.headers()
                    .add(HEADER_TOPIC, topic)
                    .add(HEADER_APP, app)
                    .add(HEADER_REQ_ID, requestId)
                    .add(HttpHeaders.Names.CONTENT_TYPE, "application/json")
                    .add(HttpHeaders.Names.CONTENT_LENGTH, content.length())
                    .add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);

            response.content().writeBytes(content.getBytes(msg.getMessageCharset()));

            out.add(response);

        } catch (Exception e) {
            logger.error(String.format("encode response error, %s", msg), e);
            TransportUtils.closeChannel(ctx.channel());
        }

    }

    // 应答消息体
    private static class ResponseBody implements Serializable {

        private ResponseStatus status;

        private String msgId;

        public ResponseBody() {
            // nothing to do.
        }

        public ResponseBody(ResponseStatus status, String msgId) {
            this.status = status;
            this.msgId = msgId;
        }

        public ResponseStatus getStatus() {
            return status;
        }

        public String getMsgId() {
            return msgId;
        }
    }

}
