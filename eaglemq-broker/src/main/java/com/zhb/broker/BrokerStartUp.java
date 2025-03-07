package com.zhb.broker;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.config.ConsumeQueueOffsetLoader;
import com.zhb.broker.config.EagleMqTopicLoader;
import com.zhb.broker.config.GlobalPropertiesLoader;
import com.zhb.broker.core.CommitLogAppendHandler;
import com.zhb.broker.core.ConsumeQueueAppendHandler;
import com.zhb.broker.core.ConsumeQueueConsumeHandler;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.netty.broker.BrokerServer;
import com.zhb.broker.slave.SlaveSyncService;
import com.zhb.broker.timewheel.RecoverManager;
import com.zhb.common.enums.BrokerClusterModeEnum;
import com.zhb.common.event.EventBus;

import java.io.IOException;

/**
 * Broker启动类
 * <p>
 * 负责EagleMQ消息队列的Broker组件初始化和启动
 * 包括配置加载、存储引擎初始化、延迟消息恢复、集群通信等核心功能
 * <p>
 * 启动流程：
 * 1. 加载配置文件和主题信息
 * 2. 初始化存储引擎和消息处理组件
 * 3. 恢复延迟消息数据
 * 4. 建立与NameServer的连接并注册
 * 5. 在集群模式下处理主从节点的同步
 * 6. 启动消息重平衡任务
 * 7. 启动Broker服务器，接收客户端请求
 */
public class BrokerStartUp {

	/**
	 * 全局配置加载器，负责加载Broker的基础配置信息
	 */
	private static GlobalPropertiesLoader globalPropertiesLoader;

	/**
	 * 主题加载器，负责加载和管理Broker中的主题信息
	 */
	private static EagleMqTopicLoader eagleMqTopicLoader;

	/**
	 * 提交日志处理器，负责处理消息的持久化写入操作
	 */
	private static CommitLogAppendHandler commitLogAppendHandler;

	/**
	 * 消费队列偏移量加载器，负责管理消费进度信息
	 */
	private static ConsumeQueueOffsetLoader consumeQueueOffsetLoader;

	/**
	 * 消费队列追加处理器，负责将消息索引添加到消费队列
	 */
	private static ConsumeQueueAppendHandler consumeQueueAppendHandler;

	/**
	 * 消费队列消费处理器，负责处理消息的消费逻辑
	 */
	private static ConsumeQueueConsumeHandler consumeQueueConsumeHandler;

	/**
	 * 从节点同步服务，用于主从模式下的数据同步
	 */
	private static SlaveSyncService slaveSyncService;

	/**
	 * 消息恢复管理，负责处理延迟消息的恢复
	 */
	private static RecoverManager recoverManager;

	/**
	 * 初始化配置逻辑
	 * 1. 创建并初始化各种加载器和处理器
	 * 2. 加载全局配置、主题信息和消费偏移量
	 * 3. 启动定时刷新任务
	 * 4. 为每个主题准备MMap文件和消费队列
	 * 5. 初始化时间轮和相关组件
	 */
	private static void initProperties() throws IOException {
		// 创建各种加载器和处理器实例
		globalPropertiesLoader = new GlobalPropertiesLoader();
		eagleMqTopicLoader = new EagleMqTopicLoader();
		consumeQueueOffsetLoader = new ConsumeQueueOffsetLoader();
		consumeQueueConsumeHandler = new ConsumeQueueConsumeHandler();
		commitLogAppendHandler = new CommitLogAppendHandler();
		consumeQueueAppendHandler = new ConsumeQueueAppendHandler();

		// 加载各种配置信息
		globalPropertiesLoader.loadProperties();
		eagleMqTopicLoader.loadProperties();
		eagleMqTopicLoader.startRefreshEagleMqTopicInfoTask();
		consumeQueueOffsetLoader.loadProperties();
		consumeQueueOffsetLoader.startRefreshConsumeQueueOffsetTask();

		// 为每个主题准备存储资源
		for (EagleMqTopicModel eagleMqTopicModel : CommonCache.getEagleMqTopicModelMap().values()) {
			String topicName = eagleMqTopicModel.getTopic();
			commitLogAppendHandler.prepareMMapLoading(topicName);
			consumeQueueAppendHandler.prepareConsumeQueue(topicName);
		}

		// 初始化时间轮组件
		CommonCache.getTimeWheelModelManager().init(new EventBus("time-wheel-event-bus"));
		CommonCache.getTimeWheelModelManager().doScanTask();

		// 在缓存中设置各种处理器，便于全局访问
		CommonCache.setConsumeQueueConsumeHandler(consumeQueueConsumeHandler);
		CommonCache.setCommitLogAppendHandler(commitLogAppendHandler);
		CommonCache.setConsumeQueueAppendHandler(consumeQueueAppendHandler);
		recoverTimeWheelData();
	}

	/**
	 * 恢复时间轮里的延迟消息数据
	 * 在Broker重启时，确保之前未处理的延迟消息能够被正确恢复
	 */
	private static void recoverTimeWheelData() {
		recoverManager = new RecoverManager();
		recoverManager.doDelayMessageRecovery();
	}

	/**
	 * 初始化和nameserver的长连接通道
	 * 1. 建立与NameServer的连接
	 * 2. 向NameServer发送Broker注册消息
	 * 3. 在主从模式下，从节点会尝试连接到主节点并开始同步
	 */
	private static void initNameServerChannel() {
		// 建立与NameServer的连接并注册Broker
		CommonCache.getNameServerClient().initConnection();
		CommonCache.getNameServerClient().sendRegistryMsg();

		// 判断是否需要建立主从同步连接
		// 只有在主从模式下且当前节点为从节点时才需要连接主节点
		if (!BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(CommonCache.getGlobalProperties().getBrokerClusterMode())
			|| "master".equals(CommonCache.getGlobalProperties().getBrokerClusterRole())) {
			return;
		}

		// 获取主节点地址并尝试连接
		String masterAddress = CommonCache.getNameServerClient().queryBrokerMasterAddress();
		if (masterAddress != null) {
			// 初始化从节点同步服务并尝试��接主节点
			slaveSyncService = new SlaveSyncService();
			CommonCache.setSlaveSyncService(slaveSyncService);
			boolean connectionStat = slaveSyncService.connectMasterBrokerNode(masterAddress);
			if (connectionStat) {
				// 连接成功后发送开始同步的消息
				slaveSyncService.sendStartSyncMsg();
			}
		}
	}

	/**
	 * 开启消费者重平衡任务
	 * 用于在消费者变化时重新分配消费队列，确保消费负载均衡
	 */
	private static void initReBalanceJob() {
		CommonCache.getConsumerInstancePool().startReBalanceJob();
	}

	/**
	 * 初始化并启动Broker服务器
	 * 创建并启动Netty服务器，监听客户端连接请求
	 */
	private static void initBrokerServer() throws InterruptedException {
		BrokerServer brokerServer = new BrokerServer(CommonCache.getGlobalProperties().getBrokerPort());
		brokerServer.startServer();
	}

	/**
	 * Broker启动入口方法
	 * 按照特定顺序初始化各个组件，启动整个Broker服务
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		// 加载配置，初始化缓存和核心组件
		initProperties();
		// 与NameServer建立连接，处理主从节点关系
		initNameServerChannel();
		// 启动消费者重平衡任务
		initReBalanceJob();
		// 最后启动Broker服务器，开始接收客户端请求
		initBrokerServer();
	}
}
