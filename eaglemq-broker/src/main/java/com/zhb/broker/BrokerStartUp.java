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
 * @Author idea
 * @Date: Created in 22:57 2024/3/26
 * @Description
 */
public class BrokerStartUp {

    private static GlobalPropertiesLoader globalPropertiesLoader;
    private static EagleMqTopicLoader eagleMqTopicLoader;
    private static CommitLogAppendHandler commitLogAppendHandler;
    private static ConsumeQueueOffsetLoader consumeQueueOffsetLoader;
    private static ConsumeQueueAppendHandler consumeQueueAppendHandler;
    private static ConsumeQueueConsumeHandler consumeQueueConsumeHandler;
    private static SlaveSyncService slaveSyncService;
    private static RecoverManager recoverManager;

    /**
     * 初始化配置逻辑
     */
    private static void initProperties() throws IOException {
        globalPropertiesLoader     = new GlobalPropertiesLoader();
        eagleMqTopicLoader         = new EagleMqTopicLoader();
        consumeQueueOffsetLoader   = new ConsumeQueueOffsetLoader();
        consumeQueueConsumeHandler = new ConsumeQueueConsumeHandler();
        commitLogAppendHandler     = new CommitLogAppendHandler();
        consumeQueueAppendHandler  = new ConsumeQueueAppendHandler();

        globalPropertiesLoader.loadProperties();
        eagleMqTopicLoader.loadProperties();
        eagleMqTopicLoader.startRefreshEagleMqTopicInfoTask();
        consumeQueueOffsetLoader.loadProperties();
        consumeQueueOffsetLoader.startRefreshConsumeQueueOffsetTask();
        for (EagleMqTopicModel eagleMqTopicModel : CommonCache.getEagleMqTopicModelMap().values()) {
            String topicName = eagleMqTopicModel.getTopic();
            commitLogAppendHandler.prepareMMapLoading(topicName);
            consumeQueueAppendHandler.prepareConsumeQueue(topicName);
        }
        CommonCache.getTimeWheelModelManager().init(new EventBus("time-wheel-event-bus"));
        CommonCache.getTimeWheelModelManager().doScanTask();
        CommonCache.setConsumeQueueConsumeHandler(consumeQueueConsumeHandler);
        CommonCache.setCommitLogAppendHandler(commitLogAppendHandler);
        CommonCache.setConsumeQueueAppendHandler(consumeQueueAppendHandler);
        recoverTimeWheelData();
    }

    /**
     * 恢复时间轮里的延迟消息数据
     */
    private static void recoverTimeWheelData() {
        recoverManager = new RecoverManager();
        recoverManager.doDelayMessageRecovery();
    }

    /**
     * 初始化和nameserver的长链接通道
     */
    private static void initNameServerChannel() {
        //主从同步链路：主节点写入数据，从节点收到同步数据（从想要收到主的通知，从是不是要连接上主节点）
        CommonCache.getNameServerClient().initConnection();
        CommonCache.getNameServerClient().sendRegistryMsg();
        //集群架构中的slave节点才需要和master建立链接
        if(!BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(CommonCache.getGlobalProperties().getBrokerClusterMode())
         || "master".equals(CommonCache.getGlobalProperties().getBrokerClusterRole())) {
            return;
        }
        String masterAddress = CommonCache.getNameServerClient().queryBrokerMasterAddress();
        if(masterAddress != null) {
            //尝试链接主broker
            slaveSyncService = new SlaveSyncService();
            CommonCache.setSlaveSyncService(slaveSyncService);
            boolean connectionStat = slaveSyncService.connectMasterBrokerNode(masterAddress);
            if(connectionStat) {
                slaveSyncService.sendStartSyncMsg();
            }
        }
    }

    //开启重平衡任务
    private static void initReBalanceJob() {
        CommonCache.getConsumerInstancePool().startReBalanceJob();
    }

    private static void initBrokerServer() throws InterruptedException {
        BrokerServer brokerServer = new BrokerServer(CommonCache.getGlobalProperties().getBrokerPort());
        brokerServer.startServer();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //加载配置 ，缓存对象的生成
        initProperties();
        initNameServerChannel();
        initReBalanceJob();
        initBrokerServer();
    }
}
