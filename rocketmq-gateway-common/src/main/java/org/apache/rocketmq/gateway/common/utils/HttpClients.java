package org.apache.rocketmq.gateway.common.utils;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class HttpClients {
    private int maxTotal = 500;
    private int defaultMaxPerRoute = 50;
    private int connectTimeout = 2000;
    private int connectionRequestTimeout = 1000;
    private int socketTimeout = 20000;
    private int maxIdleTime = 60 * 1000;
    private int idleInterval = 60 * 1000;
    private int retryCount = 1;
    private boolean redirect;
    private Map<String, Integer> maxPerRoutes = new HashMap<String, Integer>();
    private CloseableHttpClient httpClient;
    private IdleConnectionMonitorThread monitor;
    private PoolingHttpClientConnectionManager connectionManager;
    // @since 0.2.19
    private boolean sslEnabled = false;

    public HttpClients() {
    }

    public void setMaxTotal(int maxTotal) {
        if (maxTotal > 0) {
            this.maxTotal = maxTotal;
        }
    }

    public void setDefaultMaxPerRoute(int defaultMaxPerRoute) {
        if (defaultMaxPerRoute > 0) {
            this.defaultMaxPerRoute = defaultMaxPerRoute;
        }
    }

    public void setConnectTimeout(int connectTimeout) {
        if (connectTimeout > 0) {
            this.connectTimeout = connectTimeout;
        }
    }

    public void setConnectionRequestTimeout(int connectionRequestTimeout) {
        if (connectionRequestTimeout > 0) {
            this.connectionRequestTimeout = connectionRequestTimeout;
        }
    }

    public void setSocketTimeout(int socketTimeout) {
        if (socketTimeout > 0) {
            this.socketTimeout = socketTimeout;
        }
    }

    public void setRetryCount(int retryCount) {
        if (retryCount >= 0) {
            this.retryCount = retryCount;
        }
    }

    public void setRedirect(boolean redirect) {
        this.redirect = redirect;
    }

    public void setMaxPerRoutes(Map<String, Integer> maxPerRoutes) {
        this.maxPerRoutes = maxPerRoutes;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public void setIdleInterval(int idleInterval) {
        if (idleInterval > 0) {
            this.idleInterval = idleInterval;
        }
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    /**
     * 构造客户端连接
     *
     * @return
     */
    public CloseableHttpClient create() {
        if (httpClient == null) {
            synchronized (this) {
                if (httpClient == null) {
                    connectionManager = new PoolingHttpClientConnectionManager();
                    connectionManager.setMaxTotal(maxTotal);
                    connectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute);
                    if (maxPerRoutes != null && !maxPerRoutes.isEmpty()) {
                        for (Map.Entry<String, Integer> entry : maxPerRoutes.entrySet()) {
                            URL url = URL.valueOf(entry.getKey());
                            connectionManager.setMaxPerRoute(new HttpRoute(
                                    new HttpHost(url.getHost(), url.getPort() == 0 ? -1 : url.getPort(),
                                            url.getProtocol())), entry.getValue());
                        }
                    }

                    // 设置SocketConfig
                    // @since 0.2.18
                    SocketConfig socketConfig = SocketConfig.custom().setSoTimeout(socketTimeout).build();
                    connectionManager.setDefaultSocketConfig(socketConfig);

                    RequestConfig requestConfig =
                            RequestConfig.custom().setConnectionRequestTimeout(connectionRequestTimeout)
                                    .setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout)
                                    .setRedirectsEnabled(redirect).build();
                    DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(retryCount, true);

                    HttpClientBuilder httpClientBuilder = getHttpClientBuilder()
                            .setConnectionManager(connectionManager)
                            .setRetryHandler(retryHandler)
                            .setDefaultRequestConfig(requestConfig);

                    // @since 0.2.19
                    if (sslEnabled) {
                        try {
                            SSLContext sslContext = new SSLContextBuilder()
                                    .loadTrustMaterial(null, new TrustSelfSignedStrategy())
                                    .build();
                            // 不进行主机名验证
                            SSLConnectionSocketFactory sslConnectionSocketFactory
                                    = new SSLConnectionSocketFactory(sslContext, new AllowAllHostnameVerifier());

                            httpClientBuilder.setSSLSocketFactory(sslConnectionSocketFactory);

                        } catch (Exception e) {
                            throw new RuntimeException(e.getMessage(), e);
                        }
                    }

                    httpClient = httpClientBuilder.build();

                    monitor = new IdleConnectionMonitorThread();
                    monitor.start();
                }
            }
        }

        return httpClient;
    }

    /**
     * 关闭
     */
    public void close() {
        synchronized (this) {
            if (connectionManager != null) {
                connectionManager.close();
            }
            if (monitor != null) {
                monitor.shutdown();
            }
            connectionManager = null;
            monitor = null;
        }
    }

    /**
     * 获取HttpClient构造器
     *
     * @return HttpClient构造器
     * @since 0.2.19
     */
    protected HttpClientBuilder getHttpClientBuilder() {
        return HttpClientBuilder.create();
    }

    /**
     * 管理空闲的连接
     */
    protected class IdleConnectionMonitorThread extends Thread {

        private volatile boolean shutdown;

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(idleInterval);
                        if (!shutdown) {
                            // Close expired connections
                            connectionManager.closeExpiredConnections();
                            // Optionally, close idle connections
                            if (maxIdleTime > 0) {
                                connectionManager.closeIdleConnections(maxIdleTime, TimeUnit.MILLISECONDS);
                            }
                        }
                    }
                }
            } catch (InterruptedException ex) {
                // terminate
            }
        }

        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }

    }
}
