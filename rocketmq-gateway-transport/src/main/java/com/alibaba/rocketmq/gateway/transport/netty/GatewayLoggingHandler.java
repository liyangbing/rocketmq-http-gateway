package com.alibaba.rocketmq.gateway.transport.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
class GatewayLoggingHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger("gatewayLogger");

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("%s%s", MessageProduceRequest.LOG_PREFIX, msg));
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("%s%s", MessageProduceResponse.LOG_PREFIX, msg));
        }

        super.write(ctx, msg, promise);
    }

}
