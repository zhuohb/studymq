package com.zhb.common.coder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import com.zhb.common.constants.TcpConstants;

/**
 * @Author idea
 * @Date: Created in 16:03 2024/5/2
 * @Description
 */
public class TcpMsgEncoder extends MessageToByteEncoder {


    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object msg, ByteBuf out) throws Exception {
        TcpMsg tcpMsg = (TcpMsg) msg;
        out.writeShort(tcpMsg.getMagic());
        out.writeInt(tcpMsg.getCode());
        out.writeInt(tcpMsg.getLen());
        out.writeBytes(tcpMsg.getBody());
        out.writeBytes(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
    }
}
