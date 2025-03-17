package com.zhb.common.remote;

import com.zhb.common.cache.NameServerSyncFutureManager;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.constants.TcpConstants;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

/**
 * 对nameserver进行远程访问的一个客户端工具
 */
public class NameServerNettyRemoteClient {

	private String ip;
	private Integer port;

	public NameServerNettyRemoteClient(String ip, Integer port) {
		this.ip = ip;
		this.port = port;
	}

	private EventLoopGroup clientGroup = new NioEventLoopGroup();
	private Bootstrap bootstrap = new Bootstrap();
	private Channel channel;

	/**
	 * 远程连接的初始化
	 */
	public void buildConnection() {
		bootstrap.group(clientGroup);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ByteBuf delimiter = Unpooled.copiedBuffer(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
				ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024 * 8, delimiter));
				ch.pipeline().addLast(new TcpMsgDecoder());
				ch.pipeline().addLast(new TcpMsgEncoder());
				ch.pipeline().addLast(new NameServerRemoteRespHandler());
			}
		});
		ChannelFuture channelFuture = null;
		try {
			channelFuture = bootstrap.connect(ip, port).sync().addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture channelFuture) throws Exception {
					if (!channelFuture.isSuccess()) {
						throw new RuntimeException("connecting nameserver has error!");
					}
				}
			});
			//初始化建立长链接
			channel = channelFuture.channel();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public TcpMsg sendSyncMsg(TcpMsg tcpMsg, String msgId) {
		//future设计机制 jdk8里面有很多种，future，futuretask，CompletableFuture
		channel.writeAndFlush(tcpMsg);
		SyncFuture syncFuture = new SyncFuture();
		syncFuture.setMsgId(msgId);
		NameServerSyncFutureManager.put(msgId, syncFuture);
		try {
			return (TcpMsg) syncFuture.get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
