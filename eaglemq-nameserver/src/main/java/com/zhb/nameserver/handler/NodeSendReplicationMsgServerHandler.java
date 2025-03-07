package com.zhb.nameserver.handler;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.event.model.NodeReplicationAckMsgEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;

/**
 * 节点复制消息接收处理器
 * <p>
 * 负责接收和处理来自其他节点的数据复制确认消息，实现分布式集群中的数据同步反馈机制
 * 是分布式数据复制架构中的通信组件，确保数据同步过程的可靠性和完整性
 * <p>
 * 主要功能：
 * 1. 接收来自其他节点的复制确认消息
 * 2. 将网络消息解析为对应的事件对象
 * 3. 通过事件总线分发事件到对应的监听器进行处理
 * 4. 维护集群节点间的数据同步状态
 * <p>
 * 使用场景：
 * - 分布式集群中节点间的数据同步确认
 * - 数据复制流程中的确认回执处理
 * - 集群一致性状态维护
 * - 作为节点间数据复制反馈的通信入口
 */
@ChannelHandler.Sharable
public class NodeSendReplicationMsgServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	/**
	 * 构造节点复制消息接收处理器
	 * <p>
	 * 初始化处理器并设置事件总线，为处理节点间复制确认消息做准备
	 *
	 * @param eventBus 事件总线，用于分发各类事件到对应的监听器
	 */
	public NodeSendReplicationMsgServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 处理节点间复制确认消息
	 * <p>
	 * 接收并处理来自其他节点的数据复制确认消息，将消息解析为相应的事件对象，
	 * 并通过事件总线分发到对应的监听器进行处理。
	 * <p>
	 * 处理流程：
	 * 1. 接收TCP消息并提取消息代码和消息体
	 * 2. 根据消息代码将消息体解析为复制确认事件对象
	 * 3. 设置事件的通道上下文，用于后续响应
	 * 4. 通过事件总线发布事件，交由相应监听器处理
	 *
	 * @param channelHandlerContext Netty通道上下文，提供与发送节点通信的能力
	 * @param msg 接收到的原始消息对象
	 * @throws Exception 消息处理过程中可能发生的异常
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		// 将接收到的消息转换为TCP消息对象
		TcpMsg tcpMsg = (TcpMsg) msg;
		// 提取消息代码和消息体
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();

		// 根据消息代码创建对应类型的事件对象
		Event event = null;
		if (NameServerEventCode.NODE_REPLICATION_ACK_MSG.getCode() == code) {
			// 解析为节点复制确认事件
			event = JSON.parseObject(body, NodeReplicationAckMsgEvent.class);
		}

		// 设置事件的通道上下文，用于后续响应
		event.setChannelHandlerContext(channelHandlerContext);
		// 通过事件总线发布事件，交由相应的监听器处理
		eventBus.publish(event);
	}
}
