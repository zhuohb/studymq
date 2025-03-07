package com.zhb.broker.utils;

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
