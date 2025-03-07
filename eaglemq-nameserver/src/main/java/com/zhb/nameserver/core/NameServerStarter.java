package com.zhb.nameserver.core;

import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.constants.TcpConstants;
import com.zhb.common.event.EventBus;
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
import lombok.extern.slf4j.Slf4j;

/**
 * NameServer启动类
 * <p>
 * 负责初始化并启动EagleMQ消息队列的NameServer组件的网络通信服务
 * 基于Netty构建高性能网络服务，处理来自Broker和Client的各类请求
 * <p>
 * 主要功能：
 * 1. 配置Netty服务器参数
 * 2. 设置编解码器处理TCP消息
 * 3. 注册消息处理器
 * 4. 启动服务并监听指定端口
 * 5. 提供优雅关闭机制
 */
@Slf4j
public class NameServerStarter {

	/**
	 * NameServer服务监听的端口号
	 */
	private int port;

	/**
	 * 构造函数，初始化NameServer监听端口
	 *
	 * @param port 服务监听的端口号
	 */
	public NameServerStarter(int port) {
		this.port = port;
	}

	/**
	 * 启动NameServer网络服务
	 * <p>
	 * 完成以下步骤：
	 * 1. 创建并配置Netty服务器组件
	 * 2. 设置网络IO事件处理线程组
	 * 3. 配置TCP消息的编解码器
	 * 4. 注册业务逻辑处理器
	 * 5. 注册JVM关闭钩子实现优雅停机
	 * 6. 绑定端口并启动服务
	 *
	 * @throws InterruptedException 当服务启动过程被中断时抛出
	 */
	public void startServer() throws InterruptedException {
		// 处理网络IO中的连接接受(accept)事件的线程组
		NioEventLoopGroup bossGroup = new NioEventLoopGroup();
		// 处理网络IO中的数据读写(read/write)事件的线程组
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();
		// 创建服务器启动引导类
		ServerBootstrap bootstrap = new ServerBootstrap();
		// 配置线程组
		bootstrap.group(bossGroup, workerGroup);
		// 指定使用NIO传输的通道类型
		bootstrap.channel(NioServerSocketChannel.class);
		// 配置子通道的处理器
		bootstrap.childHandler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				// 创建基于分隔符的解码器，用于处理TCP��包拆包
				ByteBuf delimiter = Unpooled.copiedBuffer(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
				ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024 * 8, delimiter));
				// 添加自定义的TCP消息解码器
				ch.pipeline().addLast(new TcpMsgDecoder());
				// 添加自定义的TCP消息编码器
				ch.pipeline().addLast(new TcpMsgEncoder());
				// 添加业务逻辑处理器，并关联事件总线实现异步处理
				ch.pipeline().addLast(new TcpNettyServerHandler(new EventBus("broker-connection-")));
			}
		});

		// 注册JVM关闭钩子，实现服务的优雅停机
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			// 关闭线程组，释放资源
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			log.info("nameserver is closed");
		}));

		// 绑定端口并启动服务
		ChannelFuture channelFuture = bootstrap.bind(port).sync();
		log.info("start nameserver application on port:" + port);

		// 等待服务端口关闭，这里会阻塞主线程
		channelFuture.channel().closeFuture().sync();
	}
}
