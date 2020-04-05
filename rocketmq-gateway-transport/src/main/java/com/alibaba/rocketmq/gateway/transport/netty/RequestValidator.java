package com.alibaba.rocketmq.gateway.transport.netty;


import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest.MessageContent;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.gateway.common.protocol.MessageEntity.*;
import static org.apache.rocketmq.gateway.common.protocol.MessageProduceEntity.HEADER_DELAY_TIME_LEVEL;


@Sharable
final class RequestValidator extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RequestValidator.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof MessageProduceRequest)) {
            logger.warn(String.format("msg is not instanceof MessageProduceRequest, %s", msg.getClass().getName()));
            return;
        }

        MessageProduceRequest request = (MessageProduceRequest) msg;
        MessageContent content = request.getContent();

        ResponseStatus status = ResponseStatus.BAD_REQUEST;
        try {
            if (!HttpMethod.POST.name().equals(request.getMethod())) {
                status = ResponseStatus.METHOD_NOT_SUPPORTED;
                throw new Exception(String.format("method[%s] not supported, only support POST",
                        request.getMethod()));
            }
            if (StringUtils.isBlank(request.getTopic())) {
                throw new Exception(String.format("HEADER[%s] required.", HEADER_TOPIC));
            }
            if (StringUtils.isBlank(request.getApp())) {
                throw new Exception(String.format("HEADER[%s] required.", HEADER_APP));
            }
            if (StringUtils.isBlank(request.getRequestId())) {
                throw new Exception(String.format("HEADER[%s] required.", HEADER_REQ_ID));
            }

            if (StringUtils.isNotBlank(request.getDelayTimeLevel())) {
                try {
                    Integer.parseInt(request.getDelayTimeLevel());
                } catch (Exception e) {
                    throw new Exception(String.format("HEADER[%s] must be int.", HEADER_DELAY_TIME_LEVEL));
                }
            }

            if (content == null) {
                throw new Exception("HTTP body required, maybe you not set or its json format invalid and or charset error.");
            }
            if (StringUtils.isBlank(content.getBody())) {
                throw new Exception("message content required.");
            }

        } catch (Exception e) {
            status.setMessage(e.getMessage());
            ctx.writeAndFlush(new MessageProduceResponse(request, status));

            return;
        }

        super.channelRead(ctx, msg);

    }

}
