package com.zhb.nameserver.replication;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.MasterSlaveReplicationProperties;
import com.zhb.nameserver.enums.MasterSlaveReplicationTypeEnum;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.SlaveAckDTO;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.enums.NameServerResponseCode;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主从复制消息发送任务
 * <p>
 * 该任务负责管理NameServer主节点到从节点的数据复制流程，是实现主从架构的核心组件。
 * 根据配置的复制模式（异步、同步、半同步），采用不同的数据复制和确认策略。
 * <p>
 * 复制模式说明：
 * - 异步复制：主节点发送数据后立即返回成功，不等待从节点确认
 * - 同步复制：主节点发送数据后，等待所有从节点确认后才返回成功
 * - 半同步复制：主节点发送数据后，等待半数以上从节点确认后返回成功
 * <p>
 * 工作流程：
 * 1. 从复制消息队列中获取待复制消息
 * 2. 根据复制模式确定处理策略
 * 3. 向从节点发送复制消息
 * 4. 处理从节点的确认响应（对于同步和半同步模式）
 */
public class MasterReplicationMsgSendTask extends ReplicationTask {

	/**
	 * 构造主从复制消息发送任务
	 *
	 * @param taskName 任务名称，用于标识和日志记录
	 */
	public MasterReplicationMsgSendTask(String taskName) {
		super(taskName);
	}

	/**
	 * 启动主从复制消息发送任务
	 * <p>
	 * 该方法是任务的主循环，会持续运行并处理复制队列中的消息。根据配置的复制模式，
	 * 采用不同的消息发送和确认策略：
	 * <p>
	 * - 异步复制：直接发送同步数据，同时返回成功信号给broker
	 * - 同步复制：发送数据给所有从节点，等待所有从节点返回ACK后通知broker成功
	 * - 半同步复制：发送数据给所有从节点，等待半数以上从节点返回ACK后通知broker成功
	 */
	@Override
	public void startTask() {
		MasterSlaveReplicationProperties masterSlaveReplicationProperties = CommonCache.getNameserverProperties().getMasterSlaveReplicationProperties();
		MasterSlaveReplicationTypeEnum replicationTypeEnum = MasterSlaveReplicationTypeEnum.of(masterSlaveReplicationProperties.getType());
		//判断当前的复制模式
		//如果是异步复制，直接发送同步数据，同时返回注册成功信号给到broker节点
		//如果是同步复制，发送同步数据给到slave节点，slave节点返回ack信号，主节点收到ack信号后通知给broker注册成功
		//半同步复制其实和同步复制思路很相似
		while (true) {
			try {
				ReplicationMsgEvent replicationMsgEvent = CommonCache.getReplicationMsgQueueManager().getReplicationMsgQueue().take();
				Channel brokerChannel = replicationMsgEvent.getChannelHandlerContext().channel();
				Map<String, ChannelHandlerContext> channelHandlerContextMap = CommonCache.getReplicationChannelManager().getValidSlaveChannelMap();
				int validSlaveChannelCount = channelHandlerContextMap.keySet().size();
				if (replicationTypeEnum == MasterSlaveReplicationTypeEnum.ASYNC) {
					this.sendMsgToSlave(replicationMsgEvent);
					brokerChannel.writeAndFlush(new TcpMsg(NameServerResponseCode.REGISTRY_SUCCESS.getCode(), NameServerResponseCode.REGISTRY_SUCCESS.getDesc().getBytes()));
				} else if (replicationTypeEnum == MasterSlaveReplicationTypeEnum.SYNC) {
					//需要接收到多少个ack的次数
					this.inputMsgToAckMap(replicationMsgEvent, validSlaveChannelCount);
					this.sendMsgToSlave(replicationMsgEvent);
				} else if (replicationTypeEnum == MasterSlaveReplicationTypeEnum.HALF_SYNC) {
					this.inputMsgToAckMap(replicationMsgEvent, validSlaveChannelCount / 2);
					this.sendMsgToSlave(replicationMsgEvent);
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * 将消息记录到确认跟踪表中
	 * <p>
	 * 在同步和半同步模式下，主节点需要跟踪每条消息的确认状态。该方法将消息添加到
	 * 确认跟踪表中，并设置需要接收的确认数量。当从节点返回ACK时，会减少对应消息
	 * 的待确认计数，直到达到目标确认数后通知broker操作成功。
	 *
	 * @param replicationMsgEvent 待复制的消息事件
	 * @param needAckCount 需要接收的确认数量（同步模式为所有从节点数，半同步模式为从节点数的一半）
	 */
	private void inputMsgToAckMap(ReplicationMsgEvent replicationMsgEvent, int needAckCount) {
		CommonCache.getAckMap().put(replicationMsgEvent.getMsgId(), new SlaveAckDTO(new AtomicInteger(needAckCount), replicationMsgEvent.getChannelHandlerContext()));
	}

	/**
	 * 发送复制消息到所有从节点
	 * <p>
	 * 将复制消息发送给当前所有活跃的从节点。消息发送是非阻塞的，
	 * 发送后的确认处理根据复制模式的不同有不同的策略。
	 * <p>
	 * 发送流程：
	 * 1. 获取所有有效的从节点连接
	 * 2. 清除消息中的原通道上下文（避免序列化问题）
	 * 3. 将消息序列化并发送给每个从节点
	 *
	 * @param replicationMsgEvent 待复制的消息事件
	 */
	private void sendMsgToSlave(ReplicationMsgEvent replicationMsgEvent) {
		Map<String, ChannelHandlerContext> channelHandlerContextMap = CommonCache.getReplicationChannelManager().getValidSlaveChannelMap();
		//判断当前采用的同步模式是哪种方式
		for (String reqId : channelHandlerContextMap.keySet()) {
			replicationMsgEvent.setChannelHandlerContext(null);
			byte[] body = JSON.toJSONBytes(replicationMsgEvent);
			//异步复制，直接发送给从节点，然后告知broker注册成功
			channelHandlerContextMap.get(reqId).writeAndFlush(new TcpMsg(NameServerEventCode.MASTER_REPLICATION_MSG.getCode(), body));
		}
	}
}
