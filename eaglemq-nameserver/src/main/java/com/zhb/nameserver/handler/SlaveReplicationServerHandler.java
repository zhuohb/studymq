package com.zhb.nameserver.handler;

import com.alibaba.fastjson2.JSON;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;

/**
 * 主从复制服务端处理器
 * <p>
 * 负责在主节点上处理来自从节点的网络请求，实现主从架构下的通信和事件处理机制
 * 是主从复制架构的核心组件，充当主节点与从节点之间的网络通信桥梁
 * <p>
 * 主要功能：
 * 1. 接收从节点发送的复制请求消息
 * 2. 解析网络消息并转化为系统内部事件
 * 3. 通过事件总线分发事件到对应的监听器进行处理
 * 4. 管理从节点的连接状态和异常情况
 * <p>
 * 使用场景：
 * - 数据库主从复制同步
 * - 配置中心的数据分发
 * - 分布式系统的状态同步
 * - 主备节点间的数据一致性维护
 */
@ChannelHandler.Sharable
public class SlaveReplicationServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	/**
	 * 构造主从复制服务端处理器
	 * <p>
	 * 初始化处理器并设置事件总线，为处理从节点请求做准备
	 *
	 * @param eventBus 事件总线，用于分发各类事件到对应的监听器
	 */
	public SlaveReplicationServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 处理从节点发来的复制请求消息
	 * <p>
	 * 接收并处理来自从节点的复制请求，将消息解析为相应的事件对象，
	 * 并通过事件总线分发到对应的监听器进行处理。
	 * <p>
	 * 处理流程：
	 * 1. 接收TCP消息并提取消息代码和消息体
	 * 2. 根据消息代码将消息体解析为复制消息事件对象
	 * 3. 设置事件的通道上下文，用于后续响应
	 * 4. 通过事件总线发布事件，交由相应监听器处理
	 *
	 * @param channelHandlerContext Netty通道上下文，提供与从节点通信的能力
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
		if (NameServerEventCode.MASTER_REPLICATION_MSG.getCode() == code) {
			// 解析为复制消息事件
			event = JSON.parseObject(body, ReplicationMsgEvent.class);
		}

		// 设置事件的通道上下文，用于后续响应
		event.setChannelHandlerContext(channelHandlerContext);
		// 通过事件总线发布事件，交由相应的监听器处理
		eventBus.publish(event);
	}

	/**
	 * 处理通道非活动状态
	 * <p>
	 * 当与从节点的通信通道变为非活动状态（如连接断开）时调用，
	 * 可用于清理资源或进行重连操作。
	 *
	 * @param ctx Netty通道上下文
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// 处理从节点连接断开的情况
		// 可在此实现连接断开后的资源清理或重连策略
	}

	/**
	 * 处理通道异常
	 * <p>
	 * 当与从节点通信过程中发生异常时调用，
	 * 用于处理通信异常，记录日志，以及执行恢复操作。
	 *
	 * @param ctx Netty通道上下文
	 * @param cause 异常原因
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// 调用父类的异常处理方法
		super.exceptionCaught(ctx, cause);
		// 可在此添加额外的异常处理逻辑，如日志记录或连接重置
	}
}
