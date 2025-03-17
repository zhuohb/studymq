package com.zhb.common.coder;

import com.zhb.common.constants.TcpConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 编码器
 */
public class TcpMsgEncoder extends MessageToByteEncoder<TcpMsg> {


	@Override
	protected void encode(ChannelHandlerContext channelHandlerContext, TcpMsg msg, ByteBuf out) throws Exception {
		if (msg == null) {
			throw new IllegalArgumentException("要编码的消息为空");
		}
		if (out == null) {
			throw new IllegalArgumentException("ByteBuf为空");
		}
		out.writeShort(msg.getMagic());
		out.writeInt(msg.getCode());
		out.writeInt(msg.getLen());
		out.writeBytes(msg.getBody());
		// 写入默认解码字符字节
		out.writeBytes(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
	}
}
