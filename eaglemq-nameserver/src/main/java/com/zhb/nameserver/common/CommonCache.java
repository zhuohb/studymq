package com.zhb.nameserver.common;

import com.zhb.common.dto.NodeAckDTO;
import com.zhb.common.dto.SlaveAckDTO;
import com.zhb.nameserver.core.PropertiesLoader;
import com.zhb.nameserver.replication.ReplicationTask;
import com.zhb.nameserver.store.ReplicationChannelManager;
import com.zhb.nameserver.store.ReplicationMsgQueueManager;
import com.zhb.nameserver.store.ServiceInstanceManager;
import io.netty.channel.Channel;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NameServer全局缓存类
 * <p>
 * 维护NameServer运行过程中的各种全局组件和状态信息
 * 提供统一的访问入口，便于在不同组件间共享数据
 * 主要包含：
 * 1. 服务实例管理
 * 2. 配置信息
 * 3. 复制通道和任务
 * 4. 节点连接和确认信息
 * 5. 消息队列管理
 */
public class CommonCache {

	/**
	 * 服务实例管理器，负责管理已注册的服务实例（Broker等）
	 */
	@Getter
	private static ServiceInstanceManager serviceInstanceManager = new ServiceInstanceManager();

	/**
	 * 配置加载器，负责加载NameServer的配��信息
	 */
	@Getter
	private static PropertiesLoader propertiesLoader = new PropertiesLoader();

	/**
	 * NameServer配置属性，存储NameServer的运行参数
	 */
	@Getter
	private static NameserverProperties nameserverProperties = new NameserverProperties();

	/**
	 * 复制通道管理器，负责管理NameServer集群间的通信通道
	 */
	@Getter
	private static ReplicationChannelManager replicationChannelManager = new ReplicationChannelManager();

	/**
	 * 复制任务，用于在NameServer集群节点间复制数据
	 */
	@Getter
	private static ReplicationTask replicationTask;

	/**
	 * 与当前节点连接的通道，用于集群间通信
	 */
	@Getter
	private static Channel connectNodeChannel = null;

	/**
	 * 前置节点通信通道，在环形复制拓扑结构中连接到前一个节点
	 */
	@Getter
	private static Channel preNodeChannel = null;

	/**
	 * 复制消息队列管理器，负责管理待复制的消息队列
	 */
	@Getter
	private static ReplicationMsgQueueManager replicationMsgQueueManager = new ReplicationMsgQueueManager();

	/**
	 * 节点确认信息映射表，保存各节点的确认状态
	 */
	@Getter
	private static Map<String, NodeAckDTO> nodeAckMap = new ConcurrentHashMap<>();

	/**
	 * 从节点确认信息映射表，保存从节点的同步确认状态
	 */
	@Getter
	private static Map<String, SlaveAckDTO> ackMap = new ConcurrentHashMap<>();

	/**
	 * 设置节点确认信息映射表
	 * @param nodeAckMap 新的节点确认映射表
	 */
	public static void setNodeAckMap(Map<String, NodeAckDTO> nodeAckMap) {
		CommonCache.nodeAckMap = nodeAckMap;
	}

	/**
	 * 设置从节点确认信息映射表
	 * @param ackMap 新的从节点确认映射表
	 */
	public static void setAckMap(Map<String, SlaveAckDTO> ackMap) {
		CommonCache.ackMap = ackMap;
	}

	/**
	 * 设置前置节点通道
	 * @param preNodeChannel 前置节点通信通道
	 */
	public static void setPreNodeChannel(Channel preNodeChannel) {
		CommonCache.preNodeChannel = preNodeChannel;
	}

	/**
	 * 设置复制消息队列管理器
	 * @param replicationMsgQueueManager 复制消息队列管理器实例
	 */
	public static void setReplicationMsgQueueManager(ReplicationMsgQueueManager replicationMsgQueueManager) {
		CommonCache.replicationMsgQueueManager = replicationMsgQueueManager;
	}

	/**
	 * 设置连接节点通道
	 * @param connectNodeChannel 连接节点的通信通道
	 */
	public static void setConnectNodeChannel(Channel connectNodeChannel) {
		CommonCache.connectNodeChannel = connectNodeChannel;
	}

	/**
	 * 设置复制任务
	 * @param replicationTask 复制任务实例
	 */
	public static void setReplicationTask(ReplicationTask replicationTask) {
		CommonCache.replicationTask = replicationTask;
	}

	/**
	 * 设置复制通道管理器
	 * @param replicationChannelManager 复制通道管理器实例
	 */
	public static void setReplicationChannelManager(ReplicationChannelManager replicationChannelManager) {
		CommonCache.replicationChannelManager = replicationChannelManager;
	}

	/**
	 * 设置NameServer属性
	 * @param nameserverProperties NameServer配置属性
	 */
	public static void setNameserverProperties(NameserverProperties nameserverProperties) {
		CommonCache.nameserverProperties = nameserverProperties;
	}

	/**
	 * 设置服务实例管理器
	 * @param serviceInstanceManager 服务实例管理器
	 */
	public static void setServiceInstanceManager(ServiceInstanceManager serviceInstanceManager) {
		CommonCache.serviceInstanceManager = serviceInstanceManager;
	}

	/**
	 * 设置配置加载器
	 * @param propertiesLoader 配置加载器实例
	 */
	public static void setPropertiesLoader(PropertiesLoader propertiesLoader) {
		CommonCache.propertiesLoader = propertiesLoader;
	}
}
