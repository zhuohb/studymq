package com.zhb.broker.utils;

import java.util.concurrent.locks.ReentrantLock;


public class UnfailReentrantLock implements PutMessageLock, AckMessageLock {

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
