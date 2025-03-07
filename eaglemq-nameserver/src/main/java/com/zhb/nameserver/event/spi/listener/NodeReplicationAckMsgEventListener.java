package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.NodeAckDTO;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.enums.ReplicationMsgTypeEnum;
import com.zhb.nameserver.event.model.NodeReplicationAckMsgEvent;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;


/**
 * 节点复制确认消息监听器
 * <p>
 * 负责处理NameServer集群中的节点复制确认(ACK)消息，是环形复制拓扑中的关键组件
 * 用于确保消息在NameServer节点链中完成端到端的可靠复制
 * <p>
 * 主要功能：
 * 1. 区分和处理头节点和中间节点的ACK消息
 * 2. 在头节点，完成整个复制流程并通知Broker客户端操作成功
 * 3. 在中间节点，将ACK消息向上游传递，形成完整的确认链路
 * <p>
 * 工作原理：
 * 在环形复制拓扑中，消息顺着链路从头节点向尾节点传递
 * ACK消息则按相反方向从尾节点回到头节点，头节点最终通知Broker操作结果
 * 形成完整的请求-确认循环，确保数据在所有节点上一致
 */
@Slf4j
public class NodeReplicationAckMsgEventListener implements Listener<NodeReplicationAckMsgEvent> {


	/**
	 * 处理节点复制确认消息事件
	 * <p>
	 * 根据当前节点在复制链路中的角色执行不同的操作：
	 * - 头节点：完成整个复制流程，通知原始Broker客户端操作成功
	 * - 中间节点：将确认消息向上游节点传递，保持确认链路完整
	 * <p>
	 * 处理流程：
	 * 1. 判断当前节点是否为头节点（没有前置节点的节点）
	 * 2. 头节点：从缓存中查找对应msgId的连接信息，向Broker发送成功响应
	 * 3. 中间节点：将ACK消息传递给上游节点，继续确认链路
	 *
	 * @param event 包含确认信息的事件对象
	 * @throws Exception 当处理过程中出现错误时抛出
	 */
	@Override
	public void onReceive(NodeReplicationAckMsgEvent event) throws Exception {
		// 判断当前节点是否为头节点（没有前置节点的节点）
		boolean isHeadNode = CommonCache.getPreNodeChannel() == null;

		if (isHeadNode) {
			log.info("当前是头节点，收到下游节点ack反馈");
			// 头节点处理逻辑：通知Broker客户端整个链��同步复制完成

			// 从缓存中获取与消息ID对应的节点确认信息
			NodeAckDTO nodeAckDTO = CommonCache.getNodeAckMap().get(event.getMsgId());

			// 获取原始Broker连接通道，用于回复操作结果
			Channel brokerChannel = nodeAckDTO.getChannelHandlerContext().channel();
			if (!brokerChannel.isActive()) {
				// 检查Broker连接是否仍然活跃，防止发送到已断开的连接
				throw new RuntimeException("broker connection is broken!");
			}

			// 从缓存中移除已处理的确认消息
			CommonCache.getNodeAckMap().remove(event.getMsgId());

			// 根据消息类型，向Broker发送不同的成功响应
			if (ReplicationMsgTypeEnum.REGISTRY.getCode() == event.getType()) {
				// 处理服务注册复制完成的响应
				brokerChannel.writeAndFlush(new TcpMsg(NameServerResponseCode.REGISTRY_SUCCESS.getCode(),
					NameServerResponseCode.REGISTRY_SUCCESS.getDesc().getBytes()));
			} else if (ReplicationMsgTypeEnum.HEART_BEAT.getCode() == event.getType()) {
				// 处理心跳复制完成的响应
				brokerChannel.writeAndFlush(new TcpMsg(NameServerResponseCode.HEART_BEAT_SUCCESS.getCode(),
					NameServerResponseCode.HEART_BEAT_SUCCESS.getDesc().getBytes()));
			}
		} else {
			log.info("当前是中间节点，通知给上游节点ack回应");
			// 中间节点处理逻辑：将ACK消息向上游传递，保持确认链路

			// 将确认消息序列化并发送给上游节点，继续确认链路
			CommonCache.getPreNodeChannel().writeAndFlush(new TcpMsg(
				NameServerEventCode.NODE_REPLICATION_ACK_MSG.getCode(),
				JSON.toJSONBytes(event)));
		}
	}
}
