package com.alibaba.rocketmq.gateway.transport;


import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse;


public interface RequestProcessor {

    MessageProduceResponse process(ChannelHandlerContext ctx, MessageProduceRequest request);

}
