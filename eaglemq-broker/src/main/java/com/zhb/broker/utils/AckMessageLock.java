package com.zhb.broker.utils;

/**
 * @Author idea
 * @Date: Created in 17:15 2024/5/1
 * @Description
 */
public interface AckMessageLock {
    /**
     * 加锁
     */
    void lock();

    /**
     * 解锁
     */
    void unlock();
}
