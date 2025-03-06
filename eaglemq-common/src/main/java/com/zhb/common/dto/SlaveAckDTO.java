package com.zhb.common.dto;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author idea
 * @Date: Created in 13:57 2024/5/26
 * @Description
 */
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

    public AtomicInteger getNeedAckTime() {
        return needAckTime;
    }

    public void setNeedAckTime(AtomicInteger needAckTime) {
        this.needAckTime = needAckTime;
    }

    public ChannelHandlerContext getBrokerChannel() {
        return brokerChannel;
    }

    public void setBrokerChannel(ChannelHandlerContext brokerChannel) {
        this.brokerChannel = brokerChannel;
    }
}
