package com.zhb.nameserver.replication;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.NodeReplicationMsgEvent;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import io.netty.channel.Channel;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.NodeAckDTO;
import com.zhb.common.enums.NameServerEventCode;

/**
 * 链式复制中的非尾部节点消息发送任务
 * <p>
 * 该任务负责在链式复制架构中，将节点接收到的数据转发给链中的下一个节点。
 * 是实现可靠链式数据传播的核心组件，确保数据能够按顺序传递到链中的所有节点。
 * <p>
 * 链式复制架构说明：
 * 在链式复制模式中，消息沿着预定义的节点链依次传递。每个节点接收前一个节点的消息，
 * 处理后再转发给链中的下一个节点，直到消息到达链尾。这种模式能够减轻主节点负担，
 * 并提高整体系统的可扩展性。
 * <p>
 * 工作流程：
 * 1. 从复制消息队列中获取需要转发的消息
 * 2. 转换为适合节点间传递的消息格式
 * 3. 保存原始请求信息以便后续响应
 * 4. 将消息转发到链中的下一个节点
 */
public class NodeReplicationSendMsgTask extends ReplicationTask {

	/**
	 * 构造链式复制消息发送任务
	 *
	 * @param taskName 任务名称，用于标识和日志记录
	 */
	public NodeReplicationSendMsgTask(String taskName) {
		super(taskName);
	}

	/**
	 * 启动链式复制消息发送任务
	 * <p>
	 * 该方法是任务的主循环，持续监听并处理复制队列中的消息。当接收到新消息时，
	 * 将消息转换为节点复制消息格式，并转发给链中的下一个节点。
	 * <p>
	 * 处理流程：
	 * 1. 从复制消息队列中获取消息事件
	 * 2. 获取链中下一个节点的通信通道
	 * 3. 创建节点复制消息，保留原消息的关键信息
	 * 4. 将原始调用通道信息存入确认映射表，用于后续响应链路回调
	 * 5. 检查下一节点通道是否活跃，若活跃则转发消息
	 * <p>
	 * 注意：该任务适用于链中的非头部、非尾部节点，或仅作为链头的节点
	 */
	@Override
	void startTask() {
		while (true) {
			try {
				//如果你是头节点，不是头节点也不是尾部节点
				// 从复制队列中获取下一个待处理的消息事件
				ReplicationMsgEvent replicationMsgEvent = CommonCache.getReplicationMsgQueueManager().getReplicationMsgQueue().take();

				// 获取链中下一个节点的通信通道，用于消息转发
				Channel nextNodeChannel = CommonCache.getConnectNodeChannel();

				// 创建节点复制消息事件，转换为适合节点间传递的格式
				NodeReplicationMsgEvent nodeReplicationMsgEvent = new NodeReplicationMsgEvent();
				// 设置消息标识，保持消息在整个链路中的唯一性
				nodeReplicationMsgEvent.setMsgId(replicationMsgEvent.getMsgId());
				// 设置服务实例信息，标识消息所属的服务
				nodeReplicationMsgEvent.setServiceInstance(replicationMsgEvent.getServiceInstance());
				// 设置消息类型，决定接收节点如何处理该消息
				nodeReplicationMsgEvent.setType(replicationMsgEvent.getType());

				// 创建节点确认对象，用于后续接收确认响应
				NodeAckDTO nodeAckDTO = new NodeAckDTO();
				//broker的连接通道，保存原始的请求通道，用于后续响应回调
				nodeAckDTO.setChannelHandlerContext(replicationMsgEvent.getChannelHandlerContext());

				// 将消息ID与确认对象的映射关系存入全局缓存，用于追踪消息处理状态
				CommonCache.getNodeAckMap().put(replicationMsgEvent.getMsgId(), nodeAckDTO);

				// 检查下一节点通道是否活跃，仅在活跃状态下发送消息
				if (nextNodeChannel.isActive()) {
					// 将节点复制消息序列化并发送给下一个节点
					nextNodeChannel.writeAndFlush(new TcpMsg(NameServerEventCode.NODE_REPLICATION_MSG.getCode(), JSON.toJSONBytes(nodeReplicationMsgEvent)));
				}
				// 注：消息发送后不等待确认，继续处理下一条消息
			} catch (InterruptedException e) {
				// 如果队列操作被中断，抛出运行时异常终止任务
				throw new RuntimeException(e);
			}
		}
	}
}
