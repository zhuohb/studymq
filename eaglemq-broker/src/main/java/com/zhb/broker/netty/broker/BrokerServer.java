package com.zhb.broker.netty.broker;

import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.constants.TcpConstants;
import com.zhb.common.event.EventBus;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

/**
 * Broker服务器类
 * 负责创建并启动基于Netty的消息代理服务器
 * 处理客户端连接请求并配置相应的编解码器和消息处理器
 */
@Slf4j
public class BrokerServer {

	/**
	 * 服务器监听端口
	 */
	private int port;

	/**
	 * 构造函数
	 *
	 * @param port 服务器将监听的端口号
	 */
	public BrokerServer(int port) {
		this.port = port;
	}

	/**
	 * 启动Broker服务器
	 * 配置并初始化Netty服务器，设置事件循环组、通道类型和处理器pipeline
	 *
	 * @throws InterruptedException 服务器启动或运行过程中可能发生的中断异常
	 */
	public void startServer() throws InterruptedException {
		// 创建boss事件循环组，用于接收客户端连接
		NioEventLoopGroup bossGroup = new NioEventLoopGroup();
		// 创建worker事件循环组，用于处理已接收连接的数据读写
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();
		// 创建服务器引导程序
		ServerBootstrap bootstrap = new ServerBootstrap();
		// 配置事件循环组
		bootstrap.group(bossGroup, workerGroup);
		// 指定通道类型为NIO服务器套接��通道
		bootstrap.channel(NioServerSocketChannel.class);
		// 配置子通道处理器
		bootstrap.childHandler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				// 创建基于分隔符的解码器，用于处理粘包/拆包问题
				ByteBuf delimiter = Unpooled.copiedBuffer(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
				ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024 * 8, delimiter));
				// 添加TCP消息解码器
				ch.pipeline().addLast(new TcpMsgDecoder());
				// 添加TCP消息编码器
				ch.pipeline().addLast(new TcpMsgEncoder());
				// 添加Broker服务器消息处理器，并关联事件总线
				ch.pipeline().addLast(new BrokerServerHandler(new EventBus("broker-server-handle")));
			}
		});

		// 注册JVM关闭钩子，实现优雅停机
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			log.info("broker is closed");
		}));

		// 绑定端口并启动服务器
		ChannelFuture channelFuture = bootstrap.bind(port).sync();
		log.info("start nameserver application on port:{}", port);
		// 等待服务器通道关闭，此调用将阻塞直到服务器关闭
		channelFuture.channel().closeFuture().sync();
	}
}
