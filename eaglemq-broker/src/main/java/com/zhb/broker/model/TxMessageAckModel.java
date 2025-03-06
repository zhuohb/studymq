package com.zhb.broker.model;

import io.netty.channel.ChannelHandlerContext;
import com.zhb.common.dto.MessageDTO;

/**
 * @Author idea
 * @Date: Created at 2024/8/17
 * @Description
 */
public class TxMessageAckModel {


    private MessageDTO messageDTO;

    private ChannelHandlerContext channelHandlerContext;

    private long firstSendTime;

    public MessageDTO getMessageDTO() {
        return messageDTO;
    }

    public void setMessageDTO(MessageDTO messageDTO) {
        this.messageDTO = messageDTO;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    public void setChannelHandlerContext(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    public long getFirstSendTime() {
        return firstSendTime;
    }

    public void setFirstSendTime(long firstSendTime) {
        this.firstSendTime = firstSendTime;
    }
}
