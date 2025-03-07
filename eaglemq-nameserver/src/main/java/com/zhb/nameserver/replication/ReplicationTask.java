package com.zhb.nameserver.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 复制任务抽象基类
 * <p>
 * 该抽象类定义了NameServer中所有复制任务的基本结构和行为。
 * 复制任务是NameServer集群数据同步架构中的核心组件，负责在节点之间传输和同步数据。
 * <p>
 * 复制任务通常在独立线程中执行，支持异步操作模式。子类通过实现startTask()方法
 * 定义具体的任务执行逻辑，如数据发送、接收、确认等操作。
 * <p>
 * 主要子类包括：
 * - MasterReplicationMsgSendTask：主节点向从节点发送复制消息
 * - NodeReplicationSendMsgTask：链式复制中节点间消息传递
 * - SlaveReplicationMsgReceiveTask：从节点接收并处理复制消息
 * <p>
 * 任务执行流程：
 * 1. 创建特定类型的任务实例
 * 2. 调用startTaskAsync()方法在独立线程中启动任务
 * 3. 子类���现的startTask()方法开始执行具体复制逻辑
 */
public abstract class ReplicationTask {

	/**
	 * 日志记录器，用于记录任务执行过程中的关键信息��异常
	 */
	private final Logger logger = LoggerFactory.getLogger(ReplicationTask.class);

	/**
	 * 任务名称，用于标识任务和线程命名
	 */
	private String taskName;

	/**
	 * 创建复制任务实例
	 * <p>
	 * 初始化任务并设置任务名称，该名称会用于线程命名和日志记录，
	 * 便于在多任务环境中识别和追踪特定任务的执行情况。
	 *
	 * @param taskName 任务的唯一名称标识
	 */
	public ReplicationTask(String taskName) {
		this.taskName = taskName;
	}

	/**
	 * 异步启动复制任务
	 * <p>
	 * 创建一���新线程执行具体的复制任务逻辑。通过独立线程运行，
	 * 确保复制任务不会阻塞主线程或其他系统组件的运行。
	 * <p>
	 * 线程启动后会自动调用子类实现的startTask()方法，开始执行
	 * 实际的复制操作。线程名称会被设置为任务名称，便于识别。
	 */
	public void startTaskAsync() {
		Thread task = new Thread(() -> {
			// 记录任务启动日志
			logger.info("start job:" + taskName);
			// 调用子类具体实现的任务执行方法
			startTask();
		});
		// 设置线程名称，便于问题排查和监控
		task.setName(taskName);
		// 启动任务线程
		task.start();
	}

	/**
	 * 执行任务的具体逻辑
	 * <p>
	 * 该抽象方法定义了复制任务的核心业务逻辑，必须由子类实现。
	 * 不同类型的复制任务会根据其特定需求提供不同的实现，如：
	 * - 主节点向从节点发送数据
	 * - 节点接收并处理复制消息
	 * - 链式结构中节点间的数据转发
	 * <p>
	 * 注意：该方法通常会包含长时间运行的循环，用于持续处理复制消息，
	 * ���此一般不会返回，除非发生异常或任务被显式终止。
	 */
	abstract void startTask();
}
