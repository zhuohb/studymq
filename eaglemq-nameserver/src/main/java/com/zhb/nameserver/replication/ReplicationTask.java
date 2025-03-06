package com.zhb.nameserver.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author idea
 * @Date: Created in 10:47 2024/5/19
 * @Description
 */
public abstract class ReplicationTask {

	private final Logger logger = LoggerFactory.getLogger(ReplicationTask.class);

	private String taskName;

	public ReplicationTask(String taskName) {
		this.taskName = taskName;
	}

	public void startTaskAsync() {
		Thread task = new Thread(() -> {
			logger.info("start job:" + taskName);
			startTask();
		});
		task.setName(taskName);
		task.start();
	}

	abstract void startTask();
}
