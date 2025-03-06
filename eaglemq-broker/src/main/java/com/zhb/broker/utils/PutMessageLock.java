package com.zhb.broker.utils;

/**
 * @Author idea
 * @Date: Created in 15:56 2024/4/4
 * @Description
 */
public interface PutMessageLock {

    /**
     * 加锁
     */
    void lock();

    /**
     * 解锁
     */
    void unlock();
}
