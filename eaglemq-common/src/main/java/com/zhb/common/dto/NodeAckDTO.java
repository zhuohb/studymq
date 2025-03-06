package com.zhb.common.dto;

import io.netty.channel.ChannelHandlerContext;

/**
 * @Author idea
 * @Date: Created in 10:57 2024/6/1
 * @Description 链式复制中的ack对象
 */
public class NodeAckDTO {

    private ChannelHandlerContext channelHandlerContext;

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    public void setChannelHandlerContext(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }
}
