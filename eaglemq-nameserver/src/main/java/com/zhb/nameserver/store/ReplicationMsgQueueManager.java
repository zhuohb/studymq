package com.zhb.nameserver.store;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.TraceReplicationProperties;
import com.zhb.nameserver.enums.ReplicationModeEnum;
import com.zhb.nameserver.enums.ReplicationRoleEnum;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * 复制消息队列管理器
 * <p>
 * 该类负责管理NameServer集群中节点间复制消息的队列，是保证数据一致性传播的核心组件。
 * 根据系统配置的复制模式和节点角色，智能决定是否将消息放入复制队列进行节点间传播。
 * <p>
 * 功能特点：
 * 1. 提供线程安全的消息队列，确保高并发环境下的消息有序传递
 * 2. 根据不同的复制模式和节点角色，实现差异化的消息处理策略
 * 3. 支持多种复制架构，包括单机模式、主从复制模式和链式复制模式
 * <p>
 * 应用场景：
 * - 主从复制架构中主节点向从节点复制数据
 * - 链式复制架构中节点间的消息传递
 * - 集群数据一致性保障机制
 */
public class ReplicationMsgQueueManager {

	/**
	 * 复制消息队列
	 * <p>
	 * 采用ArrayBlockingQueue实现的有界阻塞队列，容量为5000条消息。
	 * 当队列满时，放入操作会被阻塞，确保消息不会丢失。
	 * 当队列空时，获取操作会被阻塞，提高线程资源利用率。
	 */
	private BlockingQueue<ReplicationMsgEvent> replicationMsgQueue = new ArrayBlockingQueue(5000);

	/**
	 * 获取复制消息队列
	 * <p>
	 * 返回当前的复制消息队列实例，供消息消费者（如复制任务）获取并处理队列中的消息。
	 * 队列保证了消息的有序性和线程安全性。
	 *
	 * @return 复制消息队列实例
	 */
	public BlockingQueue<ReplicationMsgEvent> getReplicationMsgQueue() {
		return replicationMsgQueue;
	}

	/**
	 * 根据复制架构决策将消息放入队列
	 * <p>
	 * 该方法根据系统配置的复制模式和节点角色，决定是否将消息放入复制队列：
	 * 1. 单机模式下，不进行任何复制处理
	 * 2. 主从复制模式下，仅主节点将消息放入队列进行复制
	 * 3. 链式复制模式下，具有下一节点的节点将消息放入队列转发
	 * <p>
	 * 通过这种智能决策机制，避��了无效的消息复制，提高了系统效率
	 *
	 * @param replicationMsgEvent 需要复制的消息事件
	 */
	public void put(ReplicationMsgEvent replicationMsgEvent) {
		// 获取当前配置的复制模式
		ReplicationModeEnum replicationModeEnum = ReplicationModeEnum.of(CommonCache.getNameserverProperties().getReplicationMode());
		if (replicationModeEnum == null) {
			// 单机架构，不做复制处理
			return;
		}
		if (replicationModeEnum == ReplicationModeEnum.MASTER_SLAVE) {
			// 主从复制模式下，判断当前节点角色
			ReplicationRoleEnum roleEnum = ReplicationRoleEnum.of(CommonCache.getNameserverProperties().getMasterSlaveReplicationProperties().getRole());
			if (roleEnum != ReplicationRoleEnum.MASTER) {
				// 非主节点不发送复制消息
				return;
			}
			// 主节点将消息放入队列进行复制
			this.sendMsgToQueue(replicationMsgEvent);
		} else if (replicationModeEnum == ReplicationModeEnum.TRACE) {
			// 链式复制模式，获取链配置信息
			TraceReplicationProperties traceReplicationProperties = CommonCache.getNameserverProperties().getTraceReplicationProperties();
			if (traceReplicationProperties.getNextNode() != null) {
				// 如果当前节点在链中有下一个节点，需要转发消息
				this.sendMsgToQueue(replicationMsgEvent);
			}
			// 链尾节点没有下一个节点，不需要转发消息
		}
	}

	/**
	 * 将消息发送到复制队列
	 * <p>
	 * 该方法是一个内部辅助方法，负责将消息实际放入阻塞队列。
	 * 使用阻塞操作确保在队列满时不会丢失消息，而是等待队列有空间。
	 * 如果线程在等待过程中被中断，会���出运行时异常。
	 *
	 * @param replicationMsgEvent 需要放入队列的复制消息事件
	 */
	private void sendMsgToQueue(ReplicationMsgEvent replicationMsgEvent) {
		try {
			// 将消息放入队列，如果队列已满则阻塞等待
			replicationMsgQueue.put(replicationMsgEvent);
		} catch (InterruptedException e) {
			// 如果线程被中断，转换为运行时异常抛出
			throw new RuntimeException(e);
		}
	}
}
