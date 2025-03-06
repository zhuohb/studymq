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
 * @Author idea
 * @Date: Created in 16:09 2024/5/18
 * @Description 集群复制服务
 */
public class ReplicationService {

	private final Logger logger = LoggerFactory.getLogger(ReplicationService.class);

	//参数的校验
	public ReplicationModeEnum checkProperties() {
		NameserverProperties nameserverProperties = CommonCache.getNameserverProperties();
		String mode = nameserverProperties.getReplicationMode();
		if (StringUtil.isNullOrEmpty(mode)) {
			logger.info("执行单机模式");
			return null;
		}
		//为空，参数不合法，抛异常
		ReplicationModeEnum replicationModeEnum = ReplicationModeEnum.of(mode);
		AssertUtils.isNotNull(replicationModeEnum, "复制模式参数异常");
		if (replicationModeEnum == ReplicationModeEnum.TRACE) {
			//链路复制
			TraceReplicationProperties traceReplicationProperties = nameserverProperties.getTraceReplicationProperties();
			AssertUtils.isNotNull(traceReplicationProperties.getPort(), "node节点的端口为空");
		} else {
			//主从复制
			MasterSlaveReplicationProperties masterSlaveReplicationProperties = nameserverProperties.getMasterSlaveReplicationProperties();
			AssertUtils.isNotBlank(masterSlaveReplicationProperties.getMaster(), "master参数不能为空");
			AssertUtils.isNotBlank(masterSlaveReplicationProperties.getRole(), "role参数不能为空");
			AssertUtils.isNotBlank(masterSlaveReplicationProperties.getType(), "type参数不能为空");
			AssertUtils.isNotNull(masterSlaveReplicationProperties.getPort(), "同步端口不能为空");
		}
		return replicationModeEnum;
	}

	//根据参数判断复制的方式 开启一个netty进程 用于做复制操作
	public void startReplicationTask(ReplicationModeEnum replicationModeEnum) {
		//单机版本，不用做处理
		if (replicationModeEnum == null) {
			return;
		}
		int port = 0;
		NameserverProperties nameserverProperties = CommonCache.getNameserverProperties();
		if (replicationModeEnum == ReplicationModeEnum.MASTER_SLAVE) {
			port = nameserverProperties.getMasterSlaveReplicationProperties().getPort();
		} else {
			replicationModeEnum = ReplicationModeEnum.TRACE;
		}

		ReplicationRoleEnum roleEnum;
		if (replicationModeEnum == ReplicationModeEnum.MASTER_SLAVE) {
			roleEnum = ReplicationRoleEnum.of(nameserverProperties.getMasterSlaveReplicationProperties().getRole());
		} else {
			String nextNode = nameserverProperties.getTraceReplicationProperties().getNextNode();
			if (StringUtil.isNullOrEmpty(nextNode)) {
				roleEnum = ReplicationRoleEnum.TAIL_NODE;
			} else {
				roleEnum = ReplicationRoleEnum.NODE;
			}
			port = nameserverProperties.getTraceReplicationProperties().getPort();
		}
		int replicationPort = port;
		//master角色，开启netty进程同步数据给master
		if (roleEnum == ReplicationRoleEnum.MASTER) {
			startNettyServerAsync(new MasterReplicationServerHandler(new EventBus("master-replication-task-")), replicationPort);
		} else if (roleEnum == ReplicationRoleEnum.SLAVE) {
			//slave角色，主动连接master角色
			String masterAddress = nameserverProperties.getMasterSlaveReplicationProperties().getMaster();
			startNettyConnAsync(new SlaveReplicationServerHandler(new EventBus("slave-replication-task-")), masterAddress);
		} else if (roleEnum == ReplicationRoleEnum.NODE) {
			String nextNodeAddress = nameserverProperties.getTraceReplicationProperties().getNextNode();
			startNettyServerAsync(new NodeWriteMsgReplicationServerHandler(new EventBus("node-write-msg-replication-task-")), replicationPort);
			startNettyConnAsync(new NodeSendReplicationMsgServerHandler(new EventBus("node-send-replication-msg-task-")), nextNodeAddress);
		} else if (roleEnum == ReplicationRoleEnum.TAIL_NODE) {
			startNettyServerAsync(new NodeWriteMsgReplicationServerHandler(new EventBus("node-write-msg-replication-task-")), replicationPort);
		}
	}


	/**
	 * 开启对目标进程的链接
	 *
	 * @param simpleChannelInboundHandler
	 * @param address
	 */
	private void startNettyConnAsync(SimpleChannelInboundHandler simpleChannelInboundHandler, String address) {
		Thread nettyConnTask = new Thread(new Runnable() {
			@Override
			public void run() {
				EventLoopGroup clientGroup = new NioEventLoopGroup();
				Bootstrap bootstrap = new Bootstrap();
				Channel channel;
				bootstrap.group(clientGroup);
				bootstrap.channel(NioSocketChannel.class);
				bootstrap.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast(new TcpMsgDecoder());
						ch.pipeline().addLast(new TcpMsgEncoder());
						ch.pipeline().addLast(simpleChannelInboundHandler);
					}
				});
				ChannelFuture channelFuture = null;
				try {
					Runtime.getRuntime().addShutdownHook(new Thread(() -> {
						clientGroup.shutdownGracefully();
						logger.info("nameserver's replication connect application is closed");
					}));
					String[] addr = address.split(":");
					channelFuture = bootstrap.connect(addr[0], Integer.parseInt(addr[1])).sync();
					//连接了master节点的channel对象，建议保存
					channel = channelFuture.channel();
					logger.info("success connected to nameserver replication!");
					CommonCache.setConnectNodeChannel(channel);
					channel.closeFuture().sync();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		});
		nettyConnTask.start();
	}

	/**
	 * 开启一个netty的进程
	 *
	 * @param simpleChannelInboundHandler
	 * @param port
	 */
	private void startNettyServerAsync(SimpleChannelInboundHandler simpleChannelInboundHandler, int port) {
		Thread nettyServerTask = new Thread(new Runnable() {
			@Override
			public void run() {
				//负责netty启动
				NioEventLoopGroup bossGroup = new NioEventLoopGroup();
				//处理网络io中的read&write事件
				NioEventLoopGroup workerGroup = new NioEventLoopGroup();
				ServerBootstrap bootstrap = new ServerBootstrap();
				bootstrap.group(bossGroup, workerGroup);
				bootstrap.channel(NioServerSocketChannel.class);
				bootstrap.childHandler(new ChannelInitializer<Channel>() {
					@Override
					protected void initChannel(Channel ch) throws Exception {
						ch.pipeline().addLast(new TcpMsgDecoder());
						ch.pipeline().addLast(new TcpMsgEncoder());
						ch.pipeline().addLast(simpleChannelInboundHandler);
					}
				});
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					bossGroup.shutdownGracefully();
					workerGroup.shutdownGracefully();
					logger.info("nameserver's replication application is closed");
				}));
				ChannelFuture channelFuture = null;
				try {
					//master-slave架构
					//写入数据的节点，这里就会开启一个服务
					//非写入数据的节点，这里就需要链接一个服务
					//trace架构
					//又要接收外界数据，又要复制数据给外界
					channelFuture = bootstrap.bind(port).sync();
					logger.info("start nameserver's replication application on port:" + port);
					//阻塞代码
					channelFuture.channel().closeFuture().sync();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		nettyServerTask.start();
	}
}
