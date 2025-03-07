package com.zhb.nameserver.handler;

import com.alibaba.fastjson2.JSON;
import com.zhb.nameserver.event.model.SlaveHeartBeatEvent;
import com.zhb.nameserver.event.model.SlaveReplicationMsgAckEvent;
import com.zhb.nameserver.event.model.StartReplicationEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;

/**
 * 主节点复制服务处理器
 * <p>
 * 负责在主节点上处理来自从节点的网络请求，实现主从架构下的通信和事件处理机制
 * 是主从复制架构的核心组件，充当主节点与从节点之间的网络通信桥梁
 * <p>
 * 主要功能：
 * 1. 接收从节点发送的各类网络请求（连接请求、心跳、数据同步确认等）
 * 2. 将网络请求解析为相应的事件对象
 * 3. 通过事件总线分发事件到对应的事件监听器进行处理
 * 4. 维护主从节点间的通信状态和连接生命周期
 * <p>
 * 使用场景：
 * - 主从复制架构下主节点接收从节点连接请求
 * - 主节点接收从节点的心跳检测消息
 * - 主节点接收从节点的数据同步确认消息
 * - 作为主从复制架构的网络层入口点
 */
@ChannelHandler.Sharable
public class MasterReplicationServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	/**
	 * 构造主节点复制服务处理器
	 * <p>
	 * 初始化处理器并设置事件总线，为处理从节点请求做准备
	 *
	 * @param eventBus 事件总线，用于分发各类事件到对应的监听器
	 */
	public MasterReplicationServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 处理从节点发送的网络消息
	 * <p>
	 * 接收并处理从节点发送的各类消息，将消息解析为相应的事件对象，
	 * 并通过事件总线分发到对应的监听器进行处理。
	 * <p>
	 * 处理流程：
	 * 1. 接收TCP消息并提取消息代码和消息体
	 * 2. 根据消息代码将消息体解析为对应的事件对象
	 * 3. 设置事件的通道上下文，用于后续响应
	 * 4. 通过事件总线发布事件，交由相应监听器处理
	 * <p>
	 * 支持的消息类型：
	 * - 复制连接请求：从节点请求与主节点建立复制连接
	 * - 心跳消息：从节点定期发送的活动状态检测
	 * - 复制确认消息：从节点确认已接收并处理主节点同步的数据
	 *
	 * @param channelHandlerContext Netty通道上下文，提供与客户端通信的能力
	 * @param msg 从节点发送的原始消息对象
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
		if (NameServerEventCode.START_REPLICATION.getCode() == code) {
			// 从节点请求建立复制连接
			event = JSON.parseObject(body, StartReplicationEvent.class);
		} else if (NameServerEventCode.SLAVE_HEART_BEAT.getCode() == code) {
			// 从节点发送心跳检测
			event = new SlaveHeartBeatEvent();
		} else if (NameServerEventCode.SLAVE_REPLICATION_ACK_MSG.getCode() == code) {
			// 从节点发送数据同步确认
			event = JSON.parseObject(body, SlaveReplicationMsgAckEvent.class);
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
		// 当前未实现具体逻辑，可扩展为检测从节点断开并进行资源清理
	}

	/**
	 * 处理通道异常
	 * <p>
	 * 当通信过程中发生异常时调用，用于异常处理和资源清理
	 *
	 * @param ctx Netty通道上下文
	 * @param cause 异常原因
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
		// 当前调用父类方法，可扩展为记录异常日志和关闭异常连接
	}
}
