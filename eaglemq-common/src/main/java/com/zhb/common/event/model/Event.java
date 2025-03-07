package com.zhb.common.event.model;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class Event {

	private String msgId;
	private ChannelHandlerContext channelHandlerContext;

}
