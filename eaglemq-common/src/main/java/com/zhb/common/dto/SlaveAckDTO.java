package com.zhb.common.dto;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;


@Setter
@Getter
public class SlaveAckDTO {

	/**
	 * 需要接收多少个从节点的ack信号
	 */
	private AtomicInteger needAckTime;
	/**
	 * broker连接主节点的channel
	 */
	private ChannelHandlerContext brokerChannel;

	public SlaveAckDTO(AtomicInteger needAckTime, ChannelHandlerContext brokerChannel) {
		this.needAckTime = needAckTime;
		this.brokerChannel = brokerChannel;
	}

}
