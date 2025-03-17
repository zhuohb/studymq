package com.zhb.common.dto;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;

/**
 * 链式复制中的ack对象
 */
@Setter
@Getter
public class NodeAckDTO {

    private ChannelHandlerContext channelHandlerContext;

}
