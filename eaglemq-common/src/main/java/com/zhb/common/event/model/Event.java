package com.zhb.common.event.model;

import io.netty.channel.ChannelHandlerContext;

/**
 * @Author idea
 * @Date: Created in 14:16 2024/5/4
 * @Description
 */
public abstract class Event {

    private String msgId;
    private ChannelHandlerContext channelHandlerContext;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    public void setChannelHandlerContext(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

}
