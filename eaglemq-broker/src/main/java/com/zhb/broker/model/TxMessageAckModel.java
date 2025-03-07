package com.zhb.broker.model;

import com.zhb.common.dto.MessageDTO;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TxMessageAckModel {


	private MessageDTO messageDTO;

	private ChannelHandlerContext channelHandlerContext;

	private long firstSendTime;

}
