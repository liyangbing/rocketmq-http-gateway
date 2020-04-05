package org.apache.rocketmq.gateway.common.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 服务
 */
public abstract class Service {

    // 锁
    protected final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // 锁
    protected final Lock mutex = rwLock.writeLock();
    // 是否启动
    protected final AtomicBoolean started = new AtomicBoolean(false);
    // 服务状态
    protected final AtomicReference<ServiceState> serviceState = new AtomicReference<ServiceState>();
    // 信号量
    protected final Object signal = new Object();

    public void start() throws Exception {
        validate();
        serviceState.set(ServiceState.WILL_START);
        beforeStart();
        mutex.lock();
        try {
            if (started.compareAndSet(false, true)) {
                try {
                    serviceState.set(ServiceState.STARTING);
                    doStart();
                    afterStart();
                    serviceState.set(ServiceState.STARTED);
                } catch (Exception e) {
                    serviceState.set(ServiceState.START_FAILED);
                    startError(e);
                    stop();
                    // 应对一些场景，需要转换一下异常
                    Exception ex = convert(e);
                    if (ex != null) {
                        throw ex;
                    }
                }
            }
        } finally {
            mutex.unlock();
        }

    }

    /**
     * 启动前
     *
     * @throws Exception
     */
    protected void beforeStart() throws Exception {

    }

    /**
     * 验证
     *
     * @throws Exception
     */
    protected void validate() throws Exception {

    }

    /**
     * 启动
     *
     * @throws Exception
     */
    protected void doStart() throws Exception {

    }

    /**
     * 启动后
     *
     * @throws Exception
     */
    protected void afterStart() throws Exception {

    }

    /**
     * 启动出错
     *
     * @param e 异常
     */
    protected void startError(Exception e) {

    }

    /**
     * 转换异常
     *
     * @param e 原异常
     * @return 目标异常
     */
    protected Exception convert(final Exception e) {
        return e;
    }

    public void stop() {
        // 设置状态将要关闭
        serviceState.set(ServiceState.WILL_STOP);
        synchronized (signal) {
            // 通知等待线程，即将关闭
            signal.notifyAll();
        }
        beforeStop();
        mutex.lock();
        try {
            if (started.compareAndSet(true, false)) {
                serviceState.set(ServiceState.STOPPING);
                doStop();
                afterStop();
                serviceState.set(ServiceState.STOPPED);
            }
        } finally {
            mutex.unlock();
        }
    }

    /**
     * 停止前
     */
    protected void beforeStop() {

    }

    /**
     * 停止
     */
    protected void doStop() {

    }

    /**
     * 停止后
     */
    protected void afterStop() {

    }

    /**
     * 将要关闭
     */
    public void willStop() {
        serviceState.set(ServiceState.WILL_STOP);
        beforeStop();
    }

    public ServiceState getServiceState() {
        return serviceState.get();
    }

    public boolean isStarted() {
        if (started.get()) {
            switch (serviceState.get()) {
                case WILL_STOP:
                case STOPPING:
                case STOPPED:
                    return false;
                default:
                    return true;
            }
        }
        return false;
    }

    /**
     * 是否关闭状态，包括启动失败，即将关闭，关闭中，关闭完成
     *
     * @return 关闭状态标示
     */
    public boolean isStopped() {
        switch (serviceState.get()) {
            case START_FAILED:
            case WILL_STOP:
            case STOPPING:
            case STOPPED:
                return true;
            default:
                return false;
        }
    }

    /**
     * 等待一段时间，如果服务已经关闭则立即返回
     *
     * @param time 时间
     */
    protected void await(final long time) {
        if (!isStarted()) {
            return;
        }
        synchronized (signal) {
            try {
                signal.wait(time);
            } catch (InterruptedException e) {
                // 当前线程终止
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 是否就绪
     *
     * @return 就绪标示
     */
    public boolean isReady() {
        return serviceState.get() == ServiceState.STARTED;
    }

    /**
     * 获取写锁
     *
     * @return 写锁
     */
    public Lock getMutex() {
        return mutex;
    }

    /**
     * 获取读锁
     *
     * @return 读锁
     */
    public Lock getReadLock() {
        return rwLock.readLock();
    }

    /**
     * 服务状态
     */
    public static enum ServiceState {
        /**
         * 准备启动
         */
        WILL_START,
        /**
         * 启动中
         */
        STARTING,
        /**
         * 启动失败
         */
        START_FAILED,
        /**
         * 启动完成
         */
        STARTED,
        /**
         * 准备关闭
         */
        WILL_STOP,
        /**
         * 关闭中
         */
        STOPPING,
        /**
         * 关闭完成
         */
        STOPPED
    }

}
