
# 解决问题

多client接入Rocketmq, 维护成本高，升级困难

# 特性

http方式生产消息、消费消息（推模式，http回调）

# 多语言接入

![link](https://github.com/liyangbing/rocketmq-http-gateway/blob/master/docs/images/business-access.jpg)

# 系统架构

![link](https://github.com/liyangbing/rocketmq-http-gateway/blob/master/docs/images/architecture.jpg)


# 快速开始

## server

1. 下载源代码，编译打包
```
mvn clean package -Dmaven.test.skip=true
```

2. 配置rocketmq nameserver
```
nameServerAddress=IP:PORT
```

3. 启动server Lancher


## client

1. 消息生产
```
curl -H "App:gateway_demo"  -H "Topic: test" -H "Request-ID: 1111" -H "Content-Type: application/json"  -d '{"key":"123", "body":{"test":"test"}}'  http://localhost:8081/gateway
```

2. 消息消费

```
SubscriptionChangeWatcher
    private static final String CONSUME_SUBSCRIPTION_DATA_DEMO = "[{\"app\":\"gateway_demo\",\"callback\":\"http://www.xxx.com/index.do\",\"tag\":\"\",\"topic\":\"test\"}]";

```

