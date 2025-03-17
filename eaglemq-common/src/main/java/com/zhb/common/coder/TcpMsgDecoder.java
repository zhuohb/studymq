package com.zhb.common.coder;

import com.zhb.common.constants.BrokerConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * 解码器
 */
public class TcpMsgDecoder extends ByteToMessageDecoder {


	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> in) throws Exception {
		if (byteBuf.readableBytes() > 2 + 4 + 4) {
			if (byteBuf.readShort() != BrokerConstants.DEFAULT_MAGIC_NUM) {
				ctx.close();
				return;
			}
			int code = byteBuf.readInt();
			int len = byteBuf.readInt();
			int readableByteLen = byteBuf.readableBytes();
			if (readableByteLen > len) {
				ctx.close();
				return;
			}
			byte[] body = new byte[len];
			byteBuf.readBytes(body);
			TcpMsg tcpMsg = new TcpMsg(code, body);
			in.add(tcpMsg);
		}
	}
}
