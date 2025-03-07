package com.zhb.broker.utils;


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
