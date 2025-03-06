package com.zhb.broker.config;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 通用的线程池配置
 */
public class CommonThreadPoolConfig {

	//专门用于将topic配置异步刷盘使用
	public static ThreadPoolExecutor refreshEagleMqTopicExecutor = new ThreadPoolExecutor(1,
		1,
		30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10), r -> {
		Thread thread = new Thread(r);
		thread.setName("refresh-eagle-mq-topic-config");
		return thread;
	});


	//专门用于将各个消费者消费进度刷新到磁盘中
	public static ThreadPoolExecutor refreshConsumeQueueOffsetExecutor = new ThreadPoolExecutor(1,
		1,
		30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10), r -> {
		Thread thread = new Thread(r);
		thread.setName("refresh-eagle-mq-topic-config");
		return thread;
	});
}
