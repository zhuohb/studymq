package com.zhb.nameserver.handler;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.NodeReplicationMsgEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;
import com.zhb.nameserver.event.model.*;

/**
 * 节点数据写入处理器
 * <p>
 * 负责接收并处理来自其他节点发送的需要同步的数据，实现分布式系统中的数据复制和写入
 * 是分布式数据同步架构中的关键组件，确保集群内数据的一致性和可靠传播
 * <p>
 * 主要功能：
 * 1. 接收来自其他节点的数据写入请求
 * 2. 将网络消息解析为对应的复制事件对象
 * 3. 记录数据来源通道，便于后续响应
 * 4. 通过事件总线分发事件到对应的监听器进行处理
 * <p>
 * 使用场景：
 * - 分布式集群中节点间的数据同步复制
 * - 主节点向从节点推送数据变更
 * - 链式复制结构中节点间的数据传播
 * - 数据中心间的跨区域数据复制
 */
@ChannelHandler.Sharable
public class NodeWriteMsgReplicationServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	/**
	 * 构造节点数据写入处理器
	 * <p>
	 * 初始化处理器并设置事件总线，为处理节点间数据写入请求做准备
	 *
	 * @param eventBus 事件总线，用于分发各类事件到对应的监听器
	 */
	public NodeWriteMsgReplicationServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 处理节点间数据写入请求
	 * <p>
	 * 接收并处理来自其他节点的数据写入请求，将消息解析为相应的事件对象，
	 * 记录数据来源通道，并通过事件总线分发到对应的监听器进行处理。
	 * <p>
	 * 处理流程：
	 * 1. 接收TCP消息并提取消息代码和消息体
	 * 2. 根据消息代码将消息体解析为节点复制消息事件对象
	 * 3. 设置事件的通道上下文，用于后续响应
	 * 4. 记录前一节点的通信通道，用于维护节点间的复制关系
	 * 5. 通过事件总线发送事件，交由相应监听器处理
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
		if (NameServerEventCode.NODE_REPLICATION_MSG.getCode() == code) {
			// 解析为节点复制消息事件
			event = JSON.parseObject(body, NodeReplicationMsgEvent.class);
		}

		// 设置事件的通道上下文，用于后续响应
		event.setChannelHandlerContext(channelHandlerContext);
		// 记录前一节点的通信通道，用于维护节点间的复制关系
		CommonCache.setPreNodeChannel(channelHandlerContext.channel());
		// 通过事件总线发布事件，交由相应的监听器处理
		eventBus.publish(event);
	}
}
