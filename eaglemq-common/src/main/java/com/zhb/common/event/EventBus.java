package com.zhb.common.event;

import com.google.common.collect.Lists;
import com.zhb.common.event.model.Event;
import com.zhb.common.utils.ReflectUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 事件总线
 * <p>
 * 该类负责管理事件和监听器的注册与发布。通过线程池异步执行事件的处理逻辑，
 * 提供了基于SPI机制的监听器自动注册功能。
 * <p>
 * 主要功能：
 * 1. 事件监听器的注册
 * 2. 事件的发布与处理
 * 3. 基于SPI机制自动加载监听器
 * 4. 使用线程池异步处理事件
 */
public class EventBus {

	/**
	 * 事件监听器映射表
	 * <p>
	 * 存储每种事件类型对应的监听器列表，支持并发访问。
	 */
	private Map<Class<? extends Event>, List<Listener>> eventListenerMap = new ConcurrentHashMap<>();

	/**
	 * 任务名称前缀
	 * <p>
	 * 用于线程池中线程的命名，便于调试和监控。
	 */
	private String taskName = "event-bus-task-";

	/**
	 * 构造函数
	 * <p>
	 * 使用指定的任务名称前缀初始化事件总线。
	 *
	 * @param taskName 任务名称前缀
	 */
	public EventBus(String taskName) {
		this.taskName = taskName;
	}

	/**
	 * 构造函数
	 * <p>
	 * 使用指定的线程池执行器初始化事件总线。
	 *
	 * @param threadPoolExecutor 线程池执行器
	 */
	public EventBus(ThreadPoolExecutor threadPoolExecutor) {
		this.threadPoolExecutor = threadPoolExecutor;
	}

	/**
	 * 线程池执行器
	 * <p>
	 * 用于异步执行事件处理任务，支持自定义线程池配置。
	 */
	private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
		10,
		100,
		3,
		TimeUnit.SECONDS,
		new ArrayBlockingQueue<>(1000),
		r -> {
			Thread thread = new Thread(r);
			thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
				@Override
				public void uncaughtException(Thread t, Throwable e) {
					e.printStackTrace();
				}
			});
			thread.setName(taskName + UUID.randomUUID().toString());
			return thread;
		});

	/**
	 * 初始化方法
	 * <p>
	 * 使用SPI机制自动加载并注册所有实现了Listener接口的监听器。
	 */
	public void init() {
		// SPI机制，JDK内置的一种提供基于文件管理接口实现的方式
		ServiceLoader<Listener> serviceLoader = ServiceLoader.load(Listener.class);
		for (Listener listener : serviceLoader) {
			Class clazz = ReflectUtils.getInterfaceT(listener, 0);
			this.registry(clazz, listener);
		}
	}

	/**
	 * 注册监听器
	 * <p>
	 * 将指定的监听器注册到对应的事件类型中。
	 *
	 * @param clazz    事件类型
	 * @param listener 监听器
	 * @param <E>      事件类型的泛型
	 */
	private <E extends Event> void registry(Class<? extends Event> clazz, Listener<E> listener) {
		List<Listener> listeners = eventListenerMap.get(clazz);
		if (CollectionUtils.isEmpty(listeners)) {
			eventListenerMap.put(clazz, Lists.newArrayList(listener));
		} else {
			listeners.add(listener);
			eventListenerMap.put(clazz, listeners);
		}
	}

	/**
	 * 发布事件
	 * <p>
	 * 将事件发布到所有注册的监听器，并使用线程池异步执行监听器的处理逻辑。
	 *
	 * @param event 要发布的事件
	 */
	public void publish(Event event) {
		List<Listener> listeners = eventListenerMap.get(event.getClass());
		threadPoolExecutor.execute(() -> {
			try {
				for (Listener listener : listeners) {
					listener.onReceive(event);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}
}
