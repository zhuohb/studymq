package com.zhb.nameserver.replication;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.MasterSlaveReplicationProperties;
import com.zhb.nameserver.common.NameserverProperties;
import com.zhb.nameserver.common.TraceReplicationProperties;
import com.zhb.nameserver.enums.ReplicationModeEnum;
import com.zhb.nameserver.enums.ReplicationRoleEnum;
import com.zhb.nameserver.handler.MasterReplicationServerHandler;
import com.zhb.nameserver.handler.NodeSendReplicationMsgServerHandler;
import com.zhb.nameserver.handler.NodeWriteMsgReplicationServerHandler;
import com.zhb.nameserver.handler.SlaveReplicationServerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.StringUtil;
import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.event.EventBus;
import com.zhb.common.utils.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群复制服务
 * <p>
 * 该服务负责管理NameServer的集群复制功能，支持三种模式的数据复制：
 * 1. 单机模式 - 不进行任何数据复制
 * 2. 主从复制模式(Master-Slave) - 主节点将数据复制到从节点
 * 3. 链式复制模式(Trace) - 数据沿预定义的节点链依次复制
 * <p>
 * 工作流程：
 * 1. 校验复制模式相关配置参数
 * 2. 根据配置的复制模式和角色，启动对应的Netty服务
 * 3. 建立节点间的通信链路，完成数据复制和同步
 * <p>
 * 复制模式说明：
 * - 单机模式：适用于开发测试场景，不进行数据复制
 * - 主从复制：主节点接收写入请求并复制到从节点，支持异步、同步和半同步复制
 * - 链式复制：数据沿预定义节点链顺序传递，减轻单点负载
 */
public class ReplicationService {

	private final Logger logger = LoggerFactory.getLogger(ReplicationService.class);

	/**
	 * 校验复制模式相关配置参数
	 * <p>
	 * 根据配置文件中的参数，检查并确定系统应该采用的复制模式。
	 * 校验过程包括：
	 * 1. 检查是否设置了复制模式，若未设置则默认为单机模式
	 * 2. 对于链式复制模式，验证节点端口配置
	 * 3. 对于主从复制模式，验证主节点地址、角色类型和复制类型等参数
	 * <p>
	 * 任何参数验证失败都会抛出异常，��止系统启动
	 *
	 * @return 确定的复制模式枚举值，若为单机模式则返回null
	 */
	public ReplicationModeEnum checkProperties() {
		// 从全局缓存中获取NameServer配置
		NameserverProperties nameserverProperties = CommonCache.getNameserverProperties();
		// 获取复制模式配置
		String mode = nameserverProperties.getReplicationMode();

		// 如果复制模式未指定，则采用单机模式
		if (StringUtil.isNullOrEmpty(mode)) {
			logger.info("执行单机模式");
			return null;
		}

		// 将字符串配置转换为枚举类型
		ReplicationModeEnum replicationModeEnum = ReplicationModeEnum.of(mode);
		// 验证复制模式是否有效
		AssertUtils.isNotNull(replicationModeEnum, "复制模式参数异常");

		if (replicationModeEnum == ReplicationModeEnum.TRACE) {
			// 链路复制模式参数校验
			TraceReplicationProperties traceReplicationProperties = nameserverProperties.getTraceReplicationProperties();
			// 确保节点监听端口已配置
			AssertUtils.isNotNull(traceReplicationProperties.getPort(), "node节点的端口为空");
		} else {
			// 主从复制模式参数校验
			MasterSlaveReplicationProperties masterSlaveReplicationProperties = nameserverProperties.getMasterSlaveReplicationProperties();
			// 验证主节点地址
			AssertUtils.isNotBlank(masterSlaveReplicationProperties.getMaster(), "master参数不能为空");
			// 验证节点角色
			AssertUtils.isNotBlank(masterSlaveReplicationProperties.getRole(), "role参数不能为空");
			// 验证复制类型（同步/异步/半同步）
			AssertUtils.isNotBlank(masterSlaveReplicationProperties.getType(), "type参数不能为空");
			// 验证同步端口
			AssertUtils.isNotNull(masterSlaveReplicationProperties.getPort(), "同步端口不能为空");
		}
		return replicationModeEnum;
	}

	/**
	 * 启动复制任务服务
	 * <p>
	 * 根据配置的复制模式和节点角色，启动对应的Netty服务：
	 * 1. 主从模式下，主节点启动服务端，从节点启动客户端连接到主节点
	 * 2. 链式模式下，根据节点位置在链中的角色，启动对应的服务：
	 *    - 中间节点同时启动服务端和客户端
	 *    - 尾节点只启动服务端接收数据
	 * <p>
	 * 启动过程会根据节点角色初始化不同的处理器，处理不同类型的复制消息
	 *
	 * @param replicationModeEnum 复制模式枚举值
	 */
	public void startReplicationTask(ReplicationModeEnum replicationModeEnum) {
		// 单机版本，不需要启动复制服务
		if (replicationModeEnum == null) {
			return;
		}

		int port = 0;
		NameserverProperties nameserverProperties = CommonCache.getNameserverProperties();

		if (replicationModeEnum == ReplicationModeEnum.MASTER_SLAVE) {
			// 主从复制模式，获取配置的同步端口
			port = nameserverProperties.getMasterSlaveReplicationProperties().getPort();
		} else {
			// 默认设置为链式复制模式
			replicationModeEnum = ReplicationModeEnum.TRACE;
		}

		ReplicationRoleEnum roleEnum;
		if (replicationModeEnum == ReplicationModeEnum.MASTER_SLAVE) {
			// 主从模式下，根据配置确定节点角色
			roleEnum = ReplicationRoleEnum.of(nameserverProperties.getMasterSlaveReplicationProperties().getRole());
		} else {
			// 链式模式下，根据是否有下一个节点确定角色
			String nextNode = nameserverProperties.getTraceReplicationProperties().getNextNode();
			if (StringUtil.isNullOrEmpty(nextNode)) {
				// 无下一节点，为链尾节点
				roleEnum = ReplicationRoleEnum.TAIL_NODE;
			} else {
				// 有下一节点，为链中间节点
				roleEnum = ReplicationRoleEnum.NODE;
			}
			// 获取链式复制的端口配置
			port = nameserverProperties.getTraceReplicationProperties().getPort();
		}

		int replicationPort = port;

		// 根据节点角色启动不同的服务
		if (roleEnum == ReplicationRoleEnum.MASTER) {
			// 主节点：启动Netty服务器，接收从节点连接
			startNettyServerAsync(new MasterReplicationServerHandler(new EventBus("master-replication-task-")), replicationPort);
		} else if (roleEnum == ReplicationRoleEnum.SLAVE) {
			// 从节点：启动Netty客户端，连接到主节点
			String masterAddress = nameserverProperties.getMasterSlaveReplicationProperties().getMaster();
			startNettyConnAsync(new SlaveReplicationServerHandler(new EventBus("slave-replication-task-")), masterAddress);
		} else if (roleEnum == ReplicationRoleEnum.NODE) {
			// 链中间节点：同时启动服务器（接收上游数据）和客户端（发送数据到下游）
			String nextNodeAddress = nameserverProperties.getTraceReplicationProperties().getNextNode();
			startNettyServerAsync(new NodeWriteMsgReplicationServerHandler(new EventBus("node-write-msg-replication-task-")), replicationPort);
			startNettyConnAsync(new NodeSendReplicationMsgServerHandler(new EventBus("node-send-replication-msg-task-")), nextNodeAddress);
		} else if (roleEnum == ReplicationRoleEnum.TAIL_NODE) {
			// 链尾节点：只启动服务器，接收上游数据
			startNettyServerAsync(new NodeWriteMsgReplicationServerHandler(new EventBus("node-write-msg-replication-task-")), replicationPort);
		}
	}

	/**
	 * 异步启动Netty客户端连接
	 * <p>
	 * 创建并启动Netty客户端，连接到指定地址的服务器。主要用于：
	 * 1. 从节点连接到主节点
	 * 2. 链式复制中，节点连接到下一个节点
	 * <p>
	 * 连接建立后，会将通道对象保存到全局缓存，供后续消息发送使用
	 *
	 * @param simpleChannelInboundHandler 处理接收消息的处理器
	 * @param address 目标服务器地址，格式为 "IP:端口"
	 */
	private void startNettyConnAsync(SimpleChannelInboundHandler simpleChannelInboundHandler, String address) {
		Thread nettyConnTask = new Thread(new Runnable() {
			@Override
			public void run() {
				// 创建客户端事件循环组
				EventLoopGroup clientGroup = new NioEventLoopGroup();
				Bootstrap bootstrap = new Bootstrap();
				Channel channel;

				// 配置客户端参数
				bootstrap.group(clientGroup);
				bootstrap.channel(NioSocketChannel.class);
				bootstrap.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						// 添加消息编解码器
						ch.pipeline().addLast(new TcpMsgDecoder());
						ch.pipeline().addLast(new TcpMsgEncoder());
						// 添加业务处理器
						ch.pipeline().addLast(simpleChannelInboundHandler);
					}
				});

				ChannelFuture channelFuture = null;
				try {
					// 添加JVM关闭钩子，优雅关闭Netty资源
					Runtime.getRuntime().addShutdownHook(new Thread(() -> {
						clientGroup.shutdownGracefully();
						logger.info("nameserver's replication connect application is closed");
					}));

					// 解析目标地址，格式为 "IP:端口"
					String[] addr = address.split(":");
					// 建立连接并等待完成
					channelFuture = bootstrap.connect(addr[0], Integer.parseInt(addr[1])).sync();

					// 获取连接通道
					channel = channelFuture.channel();
					logger.info("success connected to nameserver replication!");

					// 将连接通道保存到全局缓存，供发送消息使用
					CommonCache.setConnectNodeChannel(channel);

					// 等待连接关��
					channel.closeFuture().sync();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		});
		nettyConnTask.start();
	}

	/**
	 * 异步启动Netty服务器
	 * <p>
	 * 创建并启动Netty服务器，监听指定端口，接收其他节点的连接请求。主要用于：
	 * 1. 主节点接收从节点的连接
	 * 2. 链式复制中，��点接收上一个节点的连接
	 * <p>
	 * 服务器启动后会一直运行，处理接收到的复制消息
	 *
	 * @param simpleChannelInboundHandler 处理接收消息的处理器
	 * @param port 监听端口
	 */
	private void startNettyServerAsync(SimpleChannelInboundHandler simpleChannelInboundHandler, int port) {
		Thread nettyServerTask = new Thread(new Runnable() {
			@Override
			public void run() {
				// 创建负责接收连接的事件循环组
				NioEventLoopGroup bossGroup = new NioEventLoopGroup();
				// 创建负责处理IO事件的事件循环组
				NioEventLoopGroup workerGroup = new NioEventLoopGroup();
				ServerBootstrap bootstrap = new ServerBootstrap();

				// 配置服务器参数
				bootstrap.group(bossGroup, workerGroup);
				bootstrap.channel(NioServerSocketChannel.class);
				bootstrap.childHandler(new ChannelInitializer<Channel>() {
					@Override
					protected void initChannel(Channel ch) throws Exception {
						// 添加消息编解码器
						ch.pipeline().addLast(new TcpMsgDecoder());
						ch.pipeline().addLast(new TcpMsgEncoder());
						// 添加业务处理器
						ch.pipeline().addLast(simpleChannelInboundHandler);
					}
				});

				// 添加JVM关闭钩子，优雅关闭Netty资源
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					bossGroup.shutdownGracefully();
					workerGroup.shutdownGracefully();
					logger.info("nameserver's replication application is closed");
				}));

				ChannelFuture channelFuture = null;
				try {
					// 不同架构模式的服务器用途说明：
					// master-slave架构:
					// - 写入数据的节点会开启一个服务
					// - 非写入数据的节点需要连接到一个服务
					// trace架构:
					// - 既要接收外界数据，又要复制数据给外界

					// 绑定端口并启动服务器
					channelFuture = bootstrap.bind(port).sync();
					logger.info("start nameserver's replication application on port:" + port);

					// 阻塞直到服务器关闭
					channelFuture.channel().closeFuture().sync();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		nettyServerTask.start();
	}
}
