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
 * @Author idea
 * @Date: Created in 19:02 2024/5/26
 * @Description 下一个节点数据返回的内容接收器
 */
@ChannelHandler.Sharable
public class NodeSendReplicationMsgServerHandler extends SimpleChannelInboundHandler {

	private EventBus eventBus;

	public NodeSendReplicationMsgServerHandler(EventBus eventBus) {
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		TcpMsg tcpMsg = (TcpMsg) msg;
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();
		Event event = null;
		if (NameServerEventCode.NODE_REPLICATION_ACK_MSG.getCode() == code) {
			event = JSON.parseObject(body, NodeReplicationAckMsgEvent.class);
		}
		event.setChannelHandlerContext(channelHandlerContext);
		eventBus.publish(event);
	}
}
