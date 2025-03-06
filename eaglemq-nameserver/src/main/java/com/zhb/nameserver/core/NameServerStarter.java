package com.zhb.nameserver.core;

import com.zhb.nameserver.handler.TcpNettyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.constants.TcpConstants;
import com.zhb.common.event.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author idea
 * @Date: Created in 15:59 2024/5/2
 * @Description 基于netty启动nameserver服务
 */
public class NameServerStarter {

	private final Logger logger = LoggerFactory.getLogger(NameServerStarter.class);

	private int port;

	public NameServerStarter(int port) {
		this.port = port;
	}

	public void startServer() throws InterruptedException {
		//构建netty服务
		//注入编解码器
		//注入特定的handler
		//启动netty服务

		//处理网络io中的accept事件
		NioEventLoopGroup bossGroup = new NioEventLoopGroup();
		//处理网络io中的read&write事件
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup, workerGroup);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.childHandler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				ByteBuf delimiter = Unpooled.copiedBuffer(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
				ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024 * 8, delimiter));
				ch.pipeline().addLast(new TcpMsgDecoder());
				ch.pipeline().addLast(new TcpMsgEncoder());
				ch.pipeline().addLast(new TcpNettyServerHandler(new EventBus("broker-connection-")));
			}
		});
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			logger.info("nameserver is closed");
		}));
		ChannelFuture channelFuture = bootstrap.bind(port).sync();
		logger.info("start nameserver application on port:" + port);
		//阻塞代码
		channelFuture.channel().closeFuture().sync();
	}
}
