package com.zhb.utils;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author idea
 * @Date: Created in 15:58 2024/4/4
 * @Description
 */
public class UnfailReentrantLock implements PutMessageLock{

    private ReentrantLock reentrantLock = new ReentrantLock();


    @Override
    public void lock() {
        reentrantLock.lock();
    }

    @Override
    public void unlock() {
        reentrantLock.unlock();
    }
}
