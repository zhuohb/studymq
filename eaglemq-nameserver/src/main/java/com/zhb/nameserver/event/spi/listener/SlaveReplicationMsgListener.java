package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.Listener;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.event.model.SlaveReplicationMsgAckEvent;
import com.zhb.nameserver.store.ServiceInstance;

/**
 * 从节点专属的数据同步监听器
 * <p>
 * 负责在从节点上接收并处理来自主节点的数据同步消息，实现主从架构中的数据复制机制
 * 是主从复制模式下保证集群数据一致性的关键组件，处理主节点发送的复制指令
 * <p>
 * 主要功能：
 * 1. 接收主节点发送的数据同步消息
 * 2. 将同步的服务实例数据保存到从节点的本地存储
 * 3. 向主节点发送ACK确认消息，表明数据同步完成
 * 4. 维持从节点与主节点之间的数据一致性
 * <p>
 * 使用场景：
 * - 主从复制模式下，在从节点上接收并处理主节点同步的注册数据
 * - 作为从节点数据同步机制的入口点，确保集群数据一致性
 * - 支持高可用架构，保证主节点故障时从节点数据完整性
 */
public class SlaveReplicationMsgListener implements Listener<ReplicationMsgEvent> {

	/**
	 * 处理主节点发送的数据同步事件
	 * <p>
	 * 接收并处理主节点发送的数据同步消息，将服务实例信息保存到本地，
	 * 并向主节点发送确认(ACK)信号，完成数据同步流程。
	 * <p>
	 * 处理流程：
	 * 1. 从事件中提取服务实例信息
	 * 2. 将服务实例存储到本地服务实例管理器中
	 * 3. 创建ACK确认事件并设置相关信息
	 * 4. 通过原始通道向主节点发送ACK确认消息
	 *
	 * @param event 主节点发送的数据同步事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(ReplicationMsgEvent event) throws Exception {
		// 获取需要同步的服务实例对象
		ServiceInstance serviceInstance = event.getServiceInstance();

		// 从节点接收主节点同步数据，将服务实例保存到本地存储
		CommonCache.getServiceInstanceManager().put(serviceInstance);

		// 创建ACK确认事件，设置与原消息相同的消息ID用于关联
		SlaveReplicationMsgAckEvent slaveReplicationMsgAckEvent = new SlaveReplicationMsgAckEvent();
		slaveReplicationMsgAckEvent.setMsgId(event.getMsgId());

		// 通过原通道向主节点发送ACK确认消息，表明数据同步完成
		event.getChannelHandlerContext().channel().writeAndFlush(new TcpMsg(
			NameServerEventCode.SLAVE_REPLICATION_ACK_MSG.ordinal(),
			JSON.toJSONBytes(slaveReplicationMsgAckEvent)
		));
	}
}
