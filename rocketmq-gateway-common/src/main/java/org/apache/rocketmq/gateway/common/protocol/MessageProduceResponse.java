package org.apache.rocketmq.gateway.common.protocol;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;


public final class MessageProduceResponse extends MessageProduceEntity {

    public static final String LOG_PREFIX = "Server response: ";

    private ResponseStatus status;
    private String msgId;
    // 成功与否标识
    private Boolean success;

    public MessageProduceResponse() {
        // nothing to do.
    }

    public MessageProduceResponse(MessageProduceRequest request, ResponseStatus status) {
        this(request, status, null);
    }

    public MessageProduceResponse(MessageProduceRequest request, ResponseStatus status, String msgId) {
        super(request.internalReqId, request.getGatewayAddress());

        this.topic = request.getTopic();
        this.app = request.getApp();
        this.requestId = request.getRequestId();
        this.protocolVersion = request.getProtocolVersion();
        this.messageCharset = request.getMessageCharset();
        this.status = status;
        this.msgId = msgId;
    }

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
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

    // 响应状态
    public static final class ResponseStatus implements Serializable {
        // 消息已投递,message:SendStatus
        public static final ResponseStatus OK = new ResponseStatus(200, "OK");
        // 请求验证失败,订阅关系失败,或者缺少必要的数据项
        public static final ResponseStatus BAD_REQUEST = new ResponseStatus(400, "bad request");
        // 请求方法不支持
        public static final ResponseStatus METHOD_NOT_SUPPORTED = new ResponseStatus(405, "only supported post");
        // 服务端发生错误
        public static final ResponseStatus EXCEPTION = new ResponseStatus(500, "exception occurred.");
        // 请求过多
        public static final ResponseStatus TOO_MANY_REQ = new ResponseStatus(501, "too many request, please wait a moment.");

        // 应答编码
        private int code;
        // 应答描述
        private String message;

        public ResponseStatus() {
            // nothing to do.
        }

        public ResponseStatus(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}
