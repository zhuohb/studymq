package com.zhb.common.remote;

import com.alibaba.fastjson2.JSON;
import com.zhb.common.cache.NameServerSyncFutureManager;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.HeartBeatDTO;
import com.zhb.common.dto.PullBrokerIpRespDTO;
import com.zhb.common.dto.ServiceRegistryRespDTO;
import com.zhb.common.enums.NameServerResponseCode;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 处理对nameserver给客户端返回的数据内容
 */
@ChannelHandler.Sharable
public class NameServerRemoteRespHandler extends SimpleChannelInboundHandler {

	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
		TcpMsg tcpMsg = (TcpMsg) msg;
		int code = tcpMsg.getCode();
		byte[] body = tcpMsg.getBody();
		if (NameServerResponseCode.REGISTRY_SUCCESS.getCode() == code) {
			//msgId
			ServiceRegistryRespDTO serviceRegistryRespDTO = JSON.parseObject(tcpMsg.getBody(), ServiceRegistryRespDTO.class);
			SyncFuture syncFuture = NameServerSyncFutureManager.get(serviceRegistryRespDTO.getMsgId());
			if (syncFuture != null) {
				syncFuture.setResponse(tcpMsg);
			}
		} else if (NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode() == code) {
			ServiceRegistryRespDTO serviceRegistryRespDTO = JSON.parseObject(tcpMsg.getBody(), ServiceRegistryRespDTO.class);
			SyncFuture syncFuture = NameServerSyncFutureManager.get(serviceRegistryRespDTO.getMsgId());
			if (syncFuture != null) {
				syncFuture.setResponse(tcpMsg);
			}
		} else if (NameServerResponseCode.HEART_BEAT_SUCCESS.getCode() == code) {
			HeartBeatDTO heartBeatDTO = JSON.parseObject(tcpMsg.getBody(), HeartBeatDTO.class);
			SyncFuture syncFuture = NameServerSyncFutureManager.get(heartBeatDTO.getMsgId());
			if (syncFuture != null) {
				syncFuture.setResponse(tcpMsg);
			}
		} else if (NameServerResponseCode.PULL_BROKER_ADDRESS_SUCCESS.getCode() == code) {
			PullBrokerIpRespDTO pullBrokerIpRespDTO = JSON.parseObject(tcpMsg.getBody(), PullBrokerIpRespDTO.class);
			SyncFuture syncFuture = NameServerSyncFutureManager.get(pullBrokerIpRespDTO.getMsgId());
			if (syncFuture != null) {
				syncFuture.setResponse(tcpMsg);
			}
		}
	}
}
