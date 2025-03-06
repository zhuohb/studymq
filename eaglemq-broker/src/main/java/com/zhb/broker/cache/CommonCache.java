package com.zhb.broker.cache;

import com.zhb.broker.config.GlobalProperties;
import com.zhb.broker.core.*;
import com.zhb.broker.model.ConsumeQueueOffsetModel;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.model.TxMessageAckModel;
import com.zhb.broker.netty.nameserver.HeartBeatTaskManager;
import com.zhb.broker.netty.nameserver.NameServerClient;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.broker.rebalance.ConsumerInstancePool;
import com.zhb.broker.slave.SlaveSyncService;
import com.zhb.broker.timewheel.TimeWheelModelManager;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 统一缓存对象
 */
public class CommonCache {

	@Getter
	private static NameServerClient nameServerClient = new NameServerClient();
	@Getter
	private static GlobalProperties globalProperties = new GlobalProperties();
	@Getter
	private static List<EagleMqTopicModel> eagleMqTopicModelList = new ArrayList<>();
	@Getter
	private static ConsumeQueueOffsetModel consumeQueueOffsetModel = new ConsumeQueueOffsetModel();
	@Getter
	private static ConsumeQueueMMapFileModelManager consumeQueueMMapFileModelManager = new ConsumeQueueMMapFileModelManager();
	@Getter
	private static CommitLogMMapFileModelManager commitLogMMapFileModelManager = new CommitLogMMapFileModelManager();
	@Getter
	private static HeartBeatTaskManager heartBeatTaskManager = new HeartBeatTaskManager();
	@Getter
	private static CommitLogAppendHandler commitLogAppendHandler;
	@Getter
	private static Map<String, Map<String, List<ConsumerInstance>>> consumeHoldMap = new ConcurrentHashMap<>();
	@Getter
	private static ConsumerInstancePool consumerInstancePool = new ConsumerInstancePool();
	@Getter
	private static ConsumeQueueConsumeHandler consumeQueueConsumeHandler;
	@Getter
	private static ConsumeQueueAppendHandler consumeQueueAppendHandler;
	@Getter
	private static SlaveSyncService slaveSyncService;
	@Getter
	private static Map<String, ChannelHandlerContext> slaveChannelMap = new HashMap<>();
	@Getter
	private static TimeWheelModelManager timeWheelModelManager = new TimeWheelModelManager();
	@Getter
	private static Map<String, TxMessageAckModel> txMessageAckModelMap = new ConcurrentHashMap<>();

	public static void setTxMessageAckModelMap(Map<String, TxMessageAckModel> txMessageAckModelMap) {
		CommonCache.txMessageAckModelMap = txMessageAckModelMap;
	}

	public static void setTimeWheelModelManager(TimeWheelModelManager timeWheelModelManager) {
		CommonCache.timeWheelModelManager = timeWheelModelManager;
	}

	public static void setSlaveChannelMap(Map<String, ChannelHandlerContext> slaveChannelMap) {
		CommonCache.slaveChannelMap = slaveChannelMap;
	}

	public static void setSlaveSyncService(SlaveSyncService slaveSyncService) {
		CommonCache.slaveSyncService = slaveSyncService;
	}

	public static void setConsumerInstancePool(ConsumerInstancePool consumerInstancePool) {
		CommonCache.consumerInstancePool = consumerInstancePool;
	}

	public static void setConsumeQueueAppendHandler(ConsumeQueueAppendHandler consumeQueueAppendHandler) {
		CommonCache.consumeQueueAppendHandler = consumeQueueAppendHandler;
	}

	public static void setConsumeQueueConsumeHandler(ConsumeQueueConsumeHandler consumeQueueConsumeHandler) {
		CommonCache.consumeQueueConsumeHandler = consumeQueueConsumeHandler;
	}

	public static void setConsumeHoldMap(Map<String, Map<String, List<ConsumerInstance>>> consumeHoldMap) {
		CommonCache.consumeHoldMap = consumeHoldMap;
	}

	public static void setCommitLogAppendHandler(CommitLogAppendHandler commitLogAppendHandler) {
		CommonCache.commitLogAppendHandler = commitLogAppendHandler;
	}

	public static void setHeartBeatTaskManager(HeartBeatTaskManager heartBeatTaskManager) {
		CommonCache.heartBeatTaskManager = heartBeatTaskManager;
	}

	public static void setNameServerClient(NameServerClient nameServerClient) {
		CommonCache.nameServerClient = nameServerClient;
	}

	public static void setCommitLogMMapFileModelManager(CommitLogMMapFileModelManager commitLogMMapFileModelManager) {
		CommonCache.commitLogMMapFileModelManager = commitLogMMapFileModelManager;
	}

	public static void setConsumeQueueMMapFileModelManager(ConsumeQueueMMapFileModelManager consumeQueueMMapFileModelManager) {
		CommonCache.consumeQueueMMapFileModelManager = consumeQueueMMapFileModelManager;
	}

	public static void setGlobalProperties(GlobalProperties globalProperties) {
		CommonCache.globalProperties = globalProperties;
	}

	public static Map<String, EagleMqTopicModel> getEagleMqTopicModelMap() {
		return eagleMqTopicModelList.stream().collect(Collectors.toMap(EagleMqTopicModel::getTopic, item -> item));
	}

	public static void setEagleMqTopicModelList(List<EagleMqTopicModel> eagleMqTopicModelList) {
		CommonCache.eagleMqTopicModelList = eagleMqTopicModelList;
	}

	public static void setConsumeQueueOffsetModel(ConsumeQueueOffsetModel consumeQueueOffsetModel) {
		CommonCache.consumeQueueOffsetModel = consumeQueueOffsetModel;
	}
}
