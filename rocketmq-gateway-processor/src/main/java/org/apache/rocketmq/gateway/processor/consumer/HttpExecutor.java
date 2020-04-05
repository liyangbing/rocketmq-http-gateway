package org.apache.rocketmq.gateway.processor.consumer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

abstract class HttpExecutor {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final Charset UTF8 = Charset.forName("UTF-8");
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HEADER_ACCEPT = "Accept";

    // HTTP客户端
    protected CloseableHttpClient httpClient;
    // 是否开始请求和应答的报文日志
    protected boolean printProtocolEnabled = true;

    protected HttpExecutor() {
        this(true);
    }

    protected HttpExecutor(boolean printProtocolEnabled) {
        this.printProtocolEnabled = printProtocolEnabled;
    }


    protected String post(String path, Map<String, String> headers, byte[] content) throws Exception {
        HttpPost post = new HttpPost(path);
        String json = ContentType.APPLICATION_JSON.toString();
        post.addHeader(HEADER_CONTENT_TYPE, json);
        post.addHeader(HEADER_ACCEPT, json);
        if (headers != null && !headers.isEmpty()) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                post.addHeader(header.getKey(), header.getValue());
            }
        }

        post.setEntity(new ByteArrayEntity(content));

        RequestConfig.Builder builder = RequestConfig.custom();
        post.setConfig(builder.build());

        try {
            CloseableHttpResponse response = httpClient.execute(post);
            try {
                return handle(response);
            } catch (Exception e) {
                throw new RuntimeException("error to handle response", e);
            } finally {
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException ignored) {
                        // ignored
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("error to integrate with remote server", e);
        } finally {
            if (post != null) {
                post.releaseConnection();
            }
        }
    }

    private String handle(CloseableHttpResponse response) {
        int status = response.getStatusLine().getStatusCode();

        if (status != HttpStatus.SC_OK) {
            throw new RuntimeException(String.format("Non-OK http status, %d", status));
        }

        HttpEntity entity = response.getEntity();
        // 应答消息体
        String content = null;
        if (entity != null) {
            try {
                content = EntityUtils.toString(entity, UTF8);
            } catch (Exception e) {
                throw new RuntimeException(String.format("entity to string error, charset %s.", UTF8), e);
            }
        }

        if (printProtocolEnabled) {
            logger.info(String.format("receive response, %s", content));
        }

        return content;
    }


}
