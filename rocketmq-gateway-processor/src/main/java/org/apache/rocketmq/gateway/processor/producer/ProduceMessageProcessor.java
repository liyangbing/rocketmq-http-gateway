package org.apache.rocketmq.gateway.processor.producer;

import com.alibaba.rocketmq.gateway.transport.RequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceRequest.MessageContent;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse;
import org.apache.rocketmq.gateway.common.protocol.MessageProduceResponse.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

public final class ProduceMessageProcessor implements RequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ProduceMessageProcessor.class);

    private ProducerManager producerManager;

    @Override
    public MessageProduceResponse process(ChannelHandlerContext ctx, MessageProduceRequest request) {
        MessageProduceResponse response = validateRequest(request);
        if (response != null) {
            return response;
        }


        DefaultMQProducer producer;
        try {
            producer = producerManager.getProducer(request.getApp());

            if (producer == null) {
                ResponseStatus status = ResponseStatus.EXCEPTION;
                status.setMessage("get producer error");
                return new MessageProduceResponse(request, status);
            }

        } catch (Exception e) {
            logger.error("get producer error.", e);

            ResponseStatus status = ResponseStatus.EXCEPTION;
            status.setMessage(String.format("get producer error, %s", e.getMessage()));
            return new MessageProduceResponse(request, status);
        }

        MessageContent content = request.getContent();
        SendResult result = null;
        long startTime = System.currentTimeMillis();
        try {
            byte[] body = content.getBody().getBytes(request.getMessageCharset());
            Message message = new Message(request.getTopic(), content.getTag(), content.getKey(), body);
            if (request.getDelayTimeLevel() != null) {
                message.setDelayTimeLevel(Integer.parseInt(request.getDelayTimeLevel()));
            }
            if (request.isOneWay()) {
                producer.sendOneway(message);
            } else if (!UtilAll.isBlank(request.getOrderlyShardingKey())) {
                result = this.sendOrderly(producer, message, request.getOrderlyShardingKey());
            } else {
                result = producer.send(message);
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("encoding message error.", e);

            ResponseStatus status = ResponseStatus.EXCEPTION;
            status.setMessage(String.format("encoding message error, %s", e.getMessage()));
            return new MessageProduceResponse(request, status);
        } catch (Exception e) {
            logger.error("send message error.", e);

            ResponseStatus status = ResponseStatus.EXCEPTION;
            status.setMessage(String.format("send message error, %s", e.getMessage()));
            return new MessageProduceResponse(request, status);
        }

        ResponseStatus status = ResponseStatus.OK;
        if (result != null) {
            SendStatus sendStatus = result.getSendStatus();
            status.setMessage(sendStatus != null ? sendStatus.name() : "no send status");
        } else {
            status.setMessage("no send result");
        }

        long eclipseTime = System.currentTimeMillis() - startTime;
        if (eclipseTime > 500) {
            logger.warn(String.format("send message to broker eclipse time ge 500ms, time(ms): %s, app: %s, topic: %s",
                    eclipseTime, request.getApp(), request.getTopic()));
        }
        return new MessageProduceResponse(request, status, result != null ? result.getMsgId() : null);
    }

    /**
     * 校验请求
     *
     * @param request
     * @return
     */
    private MessageProduceResponse validateRequest(MessageProduceRequest request) {
        final String appCode = request.getApp();
        final String topicCode = request.getTopic();
        final Set<String> appTopicSubscriptionLocal = producerManager.getAppTopicSubscriptionSet();

        if (StringUtils.isBlank(appCode) || StringUtils.isBlank(topicCode)) {
            ResponseStatus status = ResponseStatus.BAD_REQUEST;
            status.setMessage(String.format("subscription[app: %s, topic: %s] is null.", appCode, topicCode));
            return new MessageProduceResponse(request, status);
        }

        String key = appCode + ProducerManager.APP_TOPIC_SUBSCRIPTION_SEPARATOR + topicCode;
        if (!appTopicSubscriptionLocal.contains(key)) {
            ResponseStatus status = ResponseStatus.BAD_REQUEST;
            status.setMessage(String.format("subscription[app: %s, topic: %s] is not found.", appCode, topicCode));
            return new MessageProduceResponse(request, status);
        }

        return null;
    }

    public void setProducerManager(ProducerManager producerManager) {
        this.producerManager = producerManager;
    }

    public SendResult sendOrderly(DefaultMQProducer defaultMQProducer, final Message message,
                                  final String shardingKey) throws MQClientException {
        if (UtilAll.isBlank(shardingKey)) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "\'shardingKey\' is blank.");
        }
        try {
            SendResult sendResult =
                    defaultMQProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object shardingKey) {
                            int select = Math.abs(shardingKey.hashCode());
                            if (select < 0) {
                                select = 0;
                            }
                            return mqs.get(select % mqs.size());
                        }
                    }, shardingKey);
            return sendResult;
        } catch (Exception e) {
            throw new MQClientException("defaultMQProducer send order exception", e);
        }
    }


}
