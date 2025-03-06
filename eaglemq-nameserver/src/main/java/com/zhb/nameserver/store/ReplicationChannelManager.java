package com.zhb.nameserver.store;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author idea
 * @Date: Created in 16:37 2024/5/18
 * @Description
 */
public class ReplicationChannelManager {

	private static Map<String, ChannelHandlerContext> channelHandlerContextMap = new ConcurrentHashMap<>();

	public Map<String, ChannelHandlerContext> getChannelHandlerContextMap() {
		return channelHandlerContextMap;
	}

	/**
	 * 返还有效连接
	 *
	 * @return
	 */
	public Map<String, ChannelHandlerContext> getValidSlaveChannelMap() {
		List<String> inValidChannelReqIdList = new ArrayList<>();
		//判断当前采用的同步模式是哪种方式
		for (String reqId : channelHandlerContextMap.keySet()) {
			Channel slaveChannel = channelHandlerContextMap.get(reqId).channel();
			if (!slaveChannel.isActive()) {
				inValidChannelReqIdList.add(reqId);
				continue;
			}
		}
		if (!inValidChannelReqIdList.isEmpty()) {
			for (String reqId : inValidChannelReqIdList) {
				//移除不可用的channel
				channelHandlerContextMap.remove(reqId);
			}
		}
		return channelHandlerContextMap;
	}

	public void put(String reqId, ChannelHandlerContext channelHandlerContext) {
		channelHandlerContextMap.put(reqId, channelHandlerContext);
	}

	public void get(String reqId) {
		channelHandlerContextMap.get(reqId);
	}
}
