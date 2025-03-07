package com.zhb.nameserver;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.TraceReplicationProperties;
import com.zhb.nameserver.core.InValidServiceRemoveTask;
import com.zhb.nameserver.core.NameServerStarter;
import com.zhb.nameserver.enums.ReplicationModeEnum;
import com.zhb.nameserver.enums.ReplicationRoleEnum;
import com.zhb.nameserver.replication.*;
import io.netty.util.internal.StringUtil;

import java.io.IOException;

/**
 * NameServer启动类
 * <p>
 * 该类是NameServer组件的入口点，负责初始化和启动整个NameServer服务。
 * 包含配置加载、复制机制初始化、无效服务清理任务启动和核心服务器启动的完整流程。
 * <p>
 * 主要功能：
 * 1. 根据配置初始化适当的复制机制（主从复制或链式复制）
 * 2. 启动相应的复制任务和心跳维护任务
 * 3. 初始化无效服务实例清理机制
 * 4. 启动NameServer核心服务
 * <p>
 * 复制模式支持：
 * - 单机模式：不进行复制
 * - 主从复制模式：主节点向从节点同步数据
 * - 链式复制模式：数据按预定��的链路逐节点传递
 *
 * @Author idea
 * @Date: Created in 15:59 2024/5/2
 * @Description 注册中心启动类
 */
public class NameServerStartUp {

	/**
	 * NameServer核心启动器实例
	 * <p>
	 * 用于初始化和启动NameServer的网络服务，处理客户端连接和请求
	 */
	private static NameServerStarter nameServerStarter;

	/**
	 * 复制服务实例
	 * <p>
	 * 负责处理节点间数据复制的核心服务组件，
	 * 根据配置的复制模式提供相应的复制能力
	 */
	private static ReplicationService replicationService = new ReplicationService();

	/**
	 * 初始化复制机制
	 * <p>
	 * 该方法根据配置加载相应的复制模式，并启动对应的复制任务。
	 * 复制机制是保证NameServer集��数据一致性的核心组件，根据不同角色和复制模式，
	 * 系统会初始化不同的复制任务。
	 * <p>
	 * 支持的复制模式和对应行为：
	 * 1. 主从复制模式
	 *    - 主节点：启动发送复制消息的任务
	 *    - 从节点：启动连接主节点和发送心跳的任务
	 * 2. 链式复制模式
	 *    - 非尾节点：启动向下一节点发送复制消息的任务
	 *    - 尾节点：不需要启动复制任务
	 */
	private static void initReplication() {
		//复制逻辑的初始化
		ReplicationModeEnum replicationModeEnum = replicationService.checkProperties();
		//这里面会根据同步模式开启不同的netty进程
		replicationService.startReplicationTask(replicationModeEnum);
		ReplicationTask replicationTask = null;
		//开启定时任务
		if (replicationModeEnum == ReplicationModeEnum.MASTER_SLAVE) {
			ReplicationRoleEnum roleEnum = ReplicationRoleEnum.of(CommonCache.getNameserverProperties().getMasterSlaveReplicationProperties().getRole());
			if (roleEnum == ReplicationRoleEnum.MASTER) {
				replicationTask = new MasterReplicationMsgSendTask("master-replication-msg-send-task");
				replicationTask.startTaskAsync();
			} else if (roleEnum == ReplicationRoleEnum.SLAVE) {
				//发送链接主节点的请求
				//开启心跳任��，发送给主节点
				replicationTask = new SlaveReplicationHeartBeatTask("slave-replication-heart-beat-send-task");
				replicationTask.startTaskAsync();
			}
		} else if (replicationModeEnum == ReplicationModeEnum.TRACE) {
			//判断当前不是一个尾节点，开启一个复制数据的异步任务
			TraceReplicationProperties traceReplicationProperties = CommonCache.getNameserverProperties().getTraceReplicationProperties();
			if (!StringUtil.isNullOrEmpty(traceReplicationProperties.getNextNode())) {
				replicationTask = new NodeReplicationSendMsgTask("node-replication-msg-send-task");
				replicationTask.startTaskAsync();
			}
		}
		CommonCache.setReplicationTask(replicationTask);
	}

	/**
	 * 初始化无效服务实例清理任务
	 * <p>
	 * 该方法创建并启动一个守护线程，用于定期检查和清理无效的服务实例。
	 * 无效服务清理是服务发现机制的重要组成部分，确保客户端始终获取健康的服务列表。
	 * <p>
	 * 服务实例可能因为网络问题、宕机或其他异常情况变得无效，
	 * 定期清理可防止客户端连接到不可用的服务实例。
	 */
	private static void initInvalidServerRemoveTask() {
		Thread inValidServiceRemoveTask = new Thread(new InValidServiceRemoveTask());
		inValidServiceRemoveTask.setName("invalid-server-remove-task");
		inValidServiceRemoveTask.start();
	}

	/**
	 * 主程序入口方法
	 * <p>
	 * 执行完整的NameServer启动流程，包括：
	 * 1. 加载配置属性
	 * 2. 初始化复制机制
	 * 3. 启动无效服务实例清理任务
	 * 4. 启动NameServer核心服务
	 * <p>
	 * 启动顺序经过精心设计，确保依赖组件先启动，
	 * 最后启动核心服务以接收外部请求。
	 *
	 * @param args 命令行参数，当前未使用
	 * @throws InterruptedException 如果线程在等待过程中被中断
	 * @throws IOException 如果启动过程中发生I/O错误
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		CommonCache.getPropertiesLoader().loadProperties();
		//获取到了集群复制的配置属性
		//master-slave 复制 ？ trace 复制？
		//如果是主从复制-》master角色-》开启一个额外的netty进程-》slave链接接入-》当数据写入master的时候，把���入的数据同步给到slave节点
		//如果是主从复制-》slave角色-》开启一个额外的netty进程-》slave端去链接master节点
		initReplication();
		initInvalidServerRemoveTask();
		nameServerStarter = new NameServerStarter(CommonCache.getNameserverProperties().getNameserverPort());
		//阻塞
		nameServerStarter.startServer();
	}
}
