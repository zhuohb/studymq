package com.zhb.nameserver.event.spi.listener;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.StartReplicationEvent;
import com.zhb.nameserver.utils.NameserverUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;

import java.net.InetSocketAddress;

/**
 * @Author idea
 * @Date: Created in 16:33 2024/5/18
 * @Description 开启同步复制监听器
 */
public class StartReplicationListener implements Listener<StartReplicationEvent> {

	@Override
	public void onReceive(StartReplicationEvent event) throws Exception {
		boolean isVerify = NameserverUtils.isVerify(event.getUser(), event.getPassword());
		ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();
		if (!isVerify) {
			TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
				NameServerResponseCode.ERROR_USER_OR_PASSWORD.getDesc().getBytes());
			channelHandlerContext.writeAndFlush(tcpMsg);
			channelHandlerContext.close();
			throw new IllegalAccessException("error account to connected!");
		}
		InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
		event.setSlaveIp(inetSocketAddress.getHostString());
		event.setSlavePort(String.valueOf(inetSocketAddress.getPort()));
		String reqId = event.getSlaveIp() + ":" + event.getSlavePort();
		channelHandlerContext.attr(AttributeKey.valueOf("reqId")).set(reqId);
		CommonCache.getReplicationChannelManager().put(reqId, channelHandlerContext);
		TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.MASTER_START_REPLICATION_ACK.getCode(), new byte[0]);
		channelHandlerContext.writeAndFlush(tcpMsg);
	}

}
