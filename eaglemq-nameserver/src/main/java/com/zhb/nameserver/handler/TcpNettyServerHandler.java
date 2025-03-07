package com.zhb.nameserver.handler;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.event.model.HeartBeatEvent;
import com.zhb.nameserver.event.model.PullBrokerIpEvent;
import com.zhb.nameserver.event.model.RegistryEvent;
import com.zhb.nameserver.event.model.UnRegistryEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.StringUtil;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.HeartBeatDTO;
import com.zhb.common.dto.PullBrokerIpDTO;
import com.zhb.common.dto.ServiceRegistryReqDTO;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.event.model.Event;
import com.zhb.nameserver.event.model.*;

import java.net.InetSocketAddress;

/**
 * NameServer TCP网络服务处理器
 * <p>
 * 负责处理来自客户端的TCP网络请求，是NameServer组件的核心网络处理器，
 * 实现了服务注册、心跳检测、Broker节点发现等功能。
 * <p>
 * 主要功能：
 * 1. 接收客户端发送的TCP网络请求
 * 2. 解析网络消息并转化为系统内部事件
 * 3. 通过事件总线分发事件到对应的监听器进行处理
 * 4. 管理客户端连接状态和异常情况
 * <p>
 * 处理的消息类型：
 * - 服务注册消息：处理服务节点的注册请求
 * - 心跳消息：维护节点存活状态
 * - Broker查询消息���提供Broker节点的发现能力
 */
@ChannelHandler.Sharable
public class TcpNettyServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	/**
	 * 构造TCP网络服务处理器
	 * <p>
	 * 初始化处理器并设置事件总线，为处理客户端请求做准备
	 *
	 * @param eventBus 事件总线，用于分发各类事件到对应的监听器
	 */
	public TcpNettyServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 处理客户端发送的网络消息
	 * <p>
	 * 系统架构工作流程：
	 * 1. 网络请求的接收(由Netty完成)
	 * 2. 事件发布器的实现（EventBus转换为event，类似Spring的事件机制或Google Guava）
	 * 3. 事件处理器的实现（Listener处理对应event）
	 * 4. 数据存储（基于Map本地内存的方式存储）
	 * <p>
	 * 处理流程：
	 * 1. 接收TCP消息并提取消息代码和消息体
	 * 2. 根据消息代码将消息转化为对应的事件对象
	 * 3. 设置事件的通道上下文，用于后续响应
	 * 4. 通过事件总线发布事件，交由相应监听器处理
	 *
	 * @param channelHandlerContext Netty通道上下文，提供与客户端通信的能力
	 * @param msg 接收到的原始消息对象
	 * @throws Exception 消息处理过程中可能发生的异常
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		TcpMsg tcpMsg = (TcpMsg) msg;
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();
		Event event = null;
		if (NameServerEventCode.REGISTRY.getCode() == code) {
			ServiceRegistryReqDTO serviceRegistryReqDTO = JSON.parseObject(body, ServiceRegistryReqDTO.class);
			RegistryEvent registryEvent = new RegistryEvent();
			registryEvent.setMsgId(serviceRegistryReqDTO.getMsgId());
			registryEvent.setPassword(serviceRegistryReqDTO.getPassword());
			registryEvent.setUser(serviceRegistryReqDTO.getUser());
			registryEvent.setAttrs(serviceRegistryReqDTO.getAttrs());
			registryEvent.setRegistryType(serviceRegistryReqDTO.getRegistryType());
			if (StringUtil.isNullOrEmpty(serviceRegistryReqDTO.getIp())) {
				InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
				registryEvent.setPort(inetSocketAddress.getPort());
				registryEvent.setIp(inetSocketAddress.getHostString());
			} else {
				registryEvent.setPort(serviceRegistryReqDTO.getPort());
				registryEvent.setIp(serviceRegistryReqDTO.getIp());
			}
			event = registryEvent;
		} else if (NameServerEventCode.HEART_BEAT.getCode() == code) {
			HeartBeatDTO heartBeatDTO = JSON.parseObject(body, HeartBeatDTO.class);
			HeartBeatEvent heartBeatEvent = new HeartBeatEvent();
			heartBeatEvent.setMsgId(heartBeatDTO.getMsgId());
			event = heartBeatEvent;
		} else if (NameServerEventCode.PULL_BROKER_IP_LIST.getCode() == code) {
			PullBrokerIpDTO pullBrokerIpDTO = JSON.parseObject(body, PullBrokerIpDTO.class);
			PullBrokerIpEvent pullBrokerIpEvent = new PullBrokerIpEvent();
			pullBrokerIpEvent.setRole(pullBrokerIpDTO.getRole());
			pullBrokerIpEvent.setBrokerClusterGroup(pullBrokerIpDTO.getBrokerClusterGroup());
			pullBrokerIpEvent.setMsgId(pullBrokerIpDTO.getMsgId());
			event = pullBrokerIpEvent;
		}
		event.setChannelHandlerContext(channelHandlerContext);
		eventBus.publish(event);
	}

	/**
	 * 处理通道非活动状态
	 * <p>
	 * 当与客户端的通信通道变为非活动状��（如连接断开）时调用，
	 * 发布注销事件使系统立即感知节点离线，避免依赖心跳超时检测带来的延迟。
	 *
	 * @param ctx Netty通道上下文
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		//如果依赖任务剔除节点，会有三个心跳周期的延迟，做到链接断开立马剔除的效果
		UnRegistryEvent unRegistryEvent = new UnRegistryEvent();
		unRegistryEvent.setChannelHandlerContext(ctx);
		eventBus.publish(unRegistryEvent);
	}

	/**
	 * 处理通道异常
	 * <p>
	 * 当与客户端通信过程中发生异常时调用，
	 * 用于处理通信异常，记录日志，以及执行恢复操作。
	 *
	 * @param ctx Netty通道上下文
	 * @param cause 异常原因
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
	}
}
