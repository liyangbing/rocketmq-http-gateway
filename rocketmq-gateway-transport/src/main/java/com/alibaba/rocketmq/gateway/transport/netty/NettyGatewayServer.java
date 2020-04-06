package com.alibaba.rocketmq.gateway.transport.netty;

import com.alibaba.rocketmq.gateway.transport.RequestProcessor;
import com.alibaba.rocketmq.gateway.transport.util.TransportUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.rocketmq.gateway.common.GatewayConfig;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse;
import org.apache.rocketmq.gateway.common.utils.NamedThreadFactory;
import org.apache.rocketmq.gateway.common.utils.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.*;

import static org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse.ResponseStatus.TOO_MANY_REQ;


public final class NettyGatewayServer extends Service {

    private static final Logger logger = LoggerFactory.getLogger(NettyGatewayServer.class);

    // 网关配置
    private final GatewayConfig gatewayConfig;
    // netty服务端
    private final ServerBootstrap serverBootstrap;
    // boss线程组
    private final EventLoopGroup eventGroupBoss;
    // worker线程组
    private final EventLoopGroup eventGroupWorker;
    // 请求处理线程
    private ExecutorService workerService;
    // 请求处理器
    private RequestProcessor processor;


    public NettyGatewayServer(GatewayConfig gatewayConfig) {
        this.gatewayConfig = gatewayConfig;
        this.serverBootstrap = new ServerBootstrap();
        this.eventGroupBoss = new NioEventLoopGroup(1, new NamedThreadFactory("boss"));
        this.eventGroupWorker = new NioEventLoopGroup(gatewayConfig.getSelectorThreads(), new NamedThreadFactory("selector"));
    }

    public void setProcessor(RequestProcessor processor) {
        this.processor = processor;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();


        workerService = new ThreadPoolExecutor(
                gatewayConfig.getWorkerThreads(),
                gatewayConfig.getWorkerThreads(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(gatewayConfig.getSendThreadPoolQueueCapacity()),
                new NamedThreadFactory("worker")
        );

        serverBootstrap.group(eventGroupBoss, eventGroupWorker)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(gatewayConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new IdleStateHandler(0, 0, gatewayConfig.getChannelMaxIdleTimeSeconds(), TimeUnit.SECONDS),
                                new NettyConnectionManageHandler(),
                                new HttpServerCodec(),
                                new HttpObjectAggregator(gatewayConfig.getHttpTransferMaxContentLength()),
                                new RequestDecoder(NettyGatewayServer.this.gatewayConfig.getGatewayAddress()),
                                new ResponseEncoder(),
                                new GatewayLoggingHandler(),
                                new RequestValidator(),
                                new NettyServerHandler()
                        );
                    }
                });

        try {
            serverBootstrap.bind().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException("server bootstrap start error.", e);
        }

        logger.info(String.format("netty gateway server started, server %s, listen %d",
                this.gatewayConfig.getGatewayAddress(), gatewayConfig.getListenPort()));
    }

    @Override
    protected void doStop() {
        super.doStop();

        try {
            eventGroupBoss.shutdownGracefully();
            eventGroupWorker.shutdownGracefully();
        } catch (Exception e) {
            logger.error("gateway server stop error.", e);
        }

        if (workerService != null) {
            workerService.shutdown();
        }

        logger.info(String.format("netty gateway server stopped, server %s", gatewayConfig.getGatewayAddress()));
    }

    private void execute(final ChannelHandlerContext ctx, final MessageProduceRequest request) {
        try {
            workerService.submit(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();

                    final MessageProduceResponse response = processor.process(ctx, request);
                    if (response != null) {
                        if (ctx.executor().inEventLoop()) {
                            ctx.writeAndFlush(response);
                        } else {
                            ctx.executor().submit(new Runnable() {
                                @Override
                                public void run() {
                                    ctx.writeAndFlush(response);
                                }
                            });
                        }
                        boolean error = isError(response);
                        if (error) {
                            logger.error("produce fail request: {}, response: {}", request, response);
                        }

                        long eclipseTime = System.currentTimeMillis() - startTime;
                        if (eclipseTime > 1000) {
                            logger.warn(String.format("send message to processor eclipse time(ms) ge 1000ms, time: %s, app: %s, topic: %s",
                                    eclipseTime, request.getApp(), request.getTopic()));
                        } else if (eclipseTime > 500) {
                            logger.warn(String.format("send message to processor eclipse time(ms) ge 500ms, time: %s, app: %s, topic: %s",
                                    eclipseTime, request.getApp(), request.getTopic()));
                        }
                    }
                }


            });
        } catch (RejectedExecutionException e) {

            // 每个线程10s打印一次
            if ((System.currentTimeMillis() % 10000) == 0) {
                logger.warn(TransportUtils.parseChannelRemoteAddr(ctx.channel()) //
                        + ", too many requests and system thread pool busy, reject it.", e);
            }

            // 过载保护
            final MessageProduceResponse response = new MessageProduceResponse(request, TOO_MANY_REQ);
            if (ctx.executor().inEventLoop()) {
                ctx.writeAndFlush(response);
            } else {
                ctx.executor().submit(new Runnable() {
                    @Override
                    public void run() {
                        ctx.writeAndFlush(response);
                    }
                });
            }

        }
    }

    private boolean isError(MessageProduceResponse response) {
        if (response == null) {
            return true;
        }
        if (response.getStatus() == null) {
            return true;
        }
        return response.getStatus().getCode() != MessageProduceResponse.ResponseStatus.OK.getCode();
    }


    @Sharable
    private class NettyServerHandler extends SimpleChannelInboundHandler<MessageProduceRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MessageProduceRequest msg) throws Exception {
            execute(ctx, msg);
        }

    }

    @Sharable
    private class NettyConnectionManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
        }


        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);
        }


        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
        }


        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }


        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
            if (event instanceof IdleStateEvent) {
                IdleStateEvent aEvent = (IdleStateEvent) event;
                if (aEvent.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = TransportUtils.parseChannelRemoteAddr(ctx.channel());
                    logger.warn("netty channel pipeline: IDLE [{}]", remoteAddress);
                    TransportUtils.closeChannel(ctx.channel());
                }
            }

            ctx.fireUserEventTriggered(event);
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = TransportUtils.parseChannelRemoteAddr(ctx.channel());
            logger.warn("netty channel pipeline: exceptionCaught {}", remoteAddress, cause);
            TransportUtils.closeChannel(ctx.channel());
        }
    }

}
