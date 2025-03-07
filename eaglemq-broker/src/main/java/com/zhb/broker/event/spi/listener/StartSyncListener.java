package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.StartSyncEvent;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.StartSyncRespDTO;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.Listener;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;

/**
 * 主从同步启动监听器
 * 负责处理从节点发起的同步请求，建立主从节点间的通信通道
 * 在主从集群模式下，用于初始化数据同步机制
 */
public class StartSyncListener implements Listener<StartSyncEvent> {

	/**
	 * 接收并处理同步启动事件
	 * 识别从节点身份，保存通信通道，并返回同步启动结果
	 *
	 * @param event 同步启动事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(StartSyncEvent event) throws Exception {
		// 获取远程连接地址信息
		InetSocketAddress inetSocketAddress = (InetSocketAddress) event.getChannelHandlerContext().channel().remoteAddress();
		// 构建请求ID，用于唯一标识从节点
		String reqId = inetSocketAddress.getAddress() + ":" + inetSocketAddress.getPort();

		// 将请求ID作为属性附加到通道上
		event.getChannelHandlerContext().attr(AttributeKey.valueOf("reqId")).set(reqId);

		// 保存从节点的通信通道到全局缓存，用于后续主动推送数据
		CommonCache.getSlaveChannelMap().put(reqId, event.getChannelHandlerContext());

		// 构建同步启动响应对象
		StartSyncRespDTO startSyncRespDTO = new StartSyncRespDTO();
		startSyncRespDTO.setMsgId(event.getMsgId());
		startSyncRespDTO.setSuccess(true);

		// 发送同步启动成功响应
		TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.START_SYNC_SUCCESS.getCode(), JSON.toJSONBytes(startSyncRespDTO));
		event.getChannelHandlerContext().writeAndFlush(responseMsg);
	}
}
