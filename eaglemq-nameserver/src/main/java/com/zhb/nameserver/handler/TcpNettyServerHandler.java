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
 * @Author idea
 * @Date: Created in 16:16 2024/5/2
 * @Description
 */
@ChannelHandler.Sharable
public class TcpNettyServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	public TcpNettyServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	//1.网络请求的接收(netty完成)
	//2.事件发布器的实现（EventBus-》event）Spring的事件，Google Guaua
	//3.事件处理器的实现（Listener-》处理event）
	//4.数据存储（基于Map本地内存的方式存储）
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

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		//如果依赖任务剔除节点，会有三个心跳周期的延迟，做到链接断开立马剔除的效果
		UnRegistryEvent unRegistryEvent = new UnRegistryEvent();
		unRegistryEvent.setChannelHandlerContext(ctx);
		eventBus.publish(unRegistryEvent);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		super.exceptionCaught(ctx, cause);
	}
}
