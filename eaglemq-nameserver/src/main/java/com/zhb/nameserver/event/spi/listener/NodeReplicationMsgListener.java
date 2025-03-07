package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.Listener;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.TraceReplicationProperties;
import com.zhb.nameserver.event.model.NodeReplicationAckMsgEvent;
import com.zhb.nameserver.event.model.NodeReplicationMsgEvent;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.store.ServiceInstance;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.Inet4Address;

/**
 * 节点复制消息监听器
 * <p>
 * 负责处理NameServer集群中的节点间复制消息，是环形复制拓扑中的关键组件
 * 接收上游节点同步的服务实例数据，维持集群中所有节点的数据一致性
 * <p>
 * 主要功能：
 * 1. 接收并处理上一节点发送的复制数据
 * 2. 将数据存入本地内存，确保节点间的数据一致性
 * 3. 根据节点角色执行不同操作：中间节点继续传递，尾部节点发送确认消息
 * <p>
 * 工作原理：
 * 在环形复制拓扑中，服务注册和心跳等数据沿着节点链路一步步传递
 * 从源节点(头节点)开始，经过所有中间节点，直到尾部节点
 * 尾部节点完成数据存储后，向上游发送ACK消息，最终返回到头节点完成整个复制流程
 */
@Slf4j
public class NodeReplicationMsgListener implements Listener<NodeReplicationMsgEvent> {
	/**
	 * 处理节点复制消息事件
	 * <p>
	 * 当从上游节点接收到复制消息时，执行以下操作：
	 * 1. 将服务实例数据存储到本地内存
	 * 2. 将消息放入复制队列，用于可能的后续处理
	 * 3. 检查节点角色并执行相应操作：
	 * - 如果是中间节点：消息会由复制管理器自动转发给下游节点
	 * - 如果是尾部节点：生成确认消息并回传给上游节点
	 * <p>
	 * 复制流程示例：
	 * node1->node2->node3->node4  (数据复制方向)
	 * node1<-node2<-node3<-node4  (确认消息方向)
	 *
	 * @param event 包含需要复制的服务实例信息的事件对象
	 * @throws Exception 当处理过程中出现错误时抛出
	 */
	@Override
	public void onReceive(NodeReplicationMsgEvent event) throws Exception {
		// 获取需要复制的服务实例信息
		ServiceInstance serviceInstance = event.getServiceInstance();

		// 接收到上一个节点同步的数据，存入本地内存
		CommonCache.getServiceInstanceManager().put(serviceInstance);

		// 创建复制消息事件并放入队列，用于后续处理或转发
		ReplicationMsgEvent replicationMsgEvent = new ReplicationMsgEvent();
		replicationMsgEvent.setServiceInstance(serviceInstance);
		replicationMsgEvent.setMsgId(event.getMsgId());
		replicationMsgEvent.setType(event.getType());
		log.info("接收到上一个节点写入的数据:{}", JSON.toJSONString(replicationMsgEvent));
		CommonCache.getReplicationMsgQueueManager().put(replicationMsgEvent);

		// 获取环形复制配置，判断当前节点角色
		TraceReplicationProperties traceReplicationProperties = CommonCache.getNameserverProperties().getTraceReplicationProperties();

		// 检查是否为尾部节点（没有下一个节点的节点）
		if (StringUtil.isNullOrEmpty(traceReplicationProperties.getNextNode())) {
			// 如果是尾部节点，不需要再给下一个节点做复制，但要返回ACK给上一个节点
			// 复制流程示例：node1->node2->node3->node4，当数据到达node4时，开始返回ACK
			log.info("当前是尾部节点，返回ack给上一个节点");

			// 创建节点复制确认消息
			NodeReplicationAckMsgEvent nodeReplicationAckMsgEvent = new NodeReplicationAckMsgEvent();
			nodeReplicationAckMsgEvent.setNodeIp(Inet4Address.getLocalHost().getHostAddress());
			nodeReplicationAckMsgEvent.setType(replicationMsgEvent.getType());
			nodeReplicationAckMsgEvent.setNodePort(traceReplicationProperties.getPort());
			nodeReplicationAckMsgEvent.setMsgId(replicationMsgEvent.getMsgId());

			// 发送确认消息给上游节点，启动确认链路
			CommonCache.getPreNodeChannel().writeAndFlush(new TcpMsg(
				NameServerEventCode.NODE_REPLICATION_ACK_MSG.getCode(),
				JSON.toJSONBytes(nodeReplicationAckMsgEvent)
			));
		}
	}
}
