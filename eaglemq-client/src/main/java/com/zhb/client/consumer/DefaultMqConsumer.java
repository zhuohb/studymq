package com.zhb.client.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections4.CollectionUtils;
import com.zhb.client.netty.BrokerRemoteRespHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.*;
import com.zhb.common.enums.*;
import com.zhb.common.event.EventBus;
import com.zhb.common.remote.BrokerNettyRemoteClient;
import com.zhb.common.remote.NameServerNettyRemoteClient;
import com.zhb.common.utils.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author idea
 * @Date: Created in 10:52 2024/6/16
 * @Description
 */
public class DefaultMqConsumer {

    private final Logger logger = LoggerFactory.getLogger(DefaultMqConsumer.class);
    private final static int EACH_BATCH_PULL_MSG_INTER = 100; //如果broker有数据，每间隔100ms拉一批
    private final static int EACH_BATCH_PULL_MSG_INTER_WHEN_NO_MSG = 1000; //如果broker无数据，每间隔1s拉一批

    private String nsIp;
    private Integer nsPort;
    private String nsUser;
    private String nsPwd;
    private String topic;
    private String consumeGroup;
    private String brokerRole = "single";
    private Integer queueId;
    private Integer batchSize;
    private String brokerClusterGroup;
    private NameServerNettyRemoteClient nameServerNettyRemoteClient;
    private List<String> brokerAddressList;
    private List<String> masterAddressList;
    private List<String> slaveAddressList;
    private MessageConsumeListener messageConsumeListener;
    private Map<String, BrokerNettyRemoteClient> brokerNettyRemoteClientMap = new ConcurrentHashMap<>();
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private CreateTopicClient createTopicClient = new CreateTopicClient();

    public void start() throws InterruptedException {
        nameServerNettyRemoteClient = new NameServerNettyRemoteClient(nsIp, nsPort);
        nameServerNettyRemoteClient.buildConnection();
        boolean isRegistrySuccess = this.doRegistry();
        if (isRegistrySuccess) {
            this.startHeartBeatTask();
            this.fetchBrokerAddress();
            this.creatRetryTopic();
            this.startConsumeMsgTask(topic);
            this.startConsumeMsgTask("retry%" + this.getConsumeGroup());
            this.startRefreshBrokerAddressJob();
            countDownLatch.await();
        }
    }

    /**
     * 创建重试主题文件
     */
    private void creatRetryTopic() {
        if(CollectionUtils.isNotEmpty(this.getBrokerAddressList())) {
            createTopicClient.createTopic("retry%" + this.getConsumeGroup(),this.getBrokerAddressList().get(0));
        } else if (CollectionUtils.isNotEmpty(this.getMasterAddressList())) {
            createTopicClient.createTopic("retry%" + this.getConsumeGroup(),this.getMasterAddressList().get(0));
        }
    }

    /**
     * 开启消费数据任务
     */
    private void startConsumeMsgTask(String topic) {
        Thread consumeTask = new Thread(() -> {
            String pullMsgTopic = topic;
            while (true) {
                try {
                    List<String> brokerNodeAddressList = new ArrayList<>();
                    //可以通过调整角色值来控制从哪个broker节点拉数据
                    if ("single".equals(brokerRole)) {
                        brokerNodeAddressList = this.getBrokerAddressList();
                    } else if ("master".equals(brokerRole)) {
                        brokerNodeAddressList = this.getMasterAddressList();
                    } else if ("slave".equals(brokerRole)) {
                        brokerNodeAddressList = this.getSlaveAddressList();
                    }
                    if (CollectionUtils.isEmpty(brokerNodeAddressList)) {
                        TimeUnit.MILLISECONDS.sleep(EACH_BATCH_PULL_MSG_INTER_WHEN_NO_MSG);
                        logger.warn("broker address is empty!");
                        continue;
                    }
                    for (String brokerNodeAddress : brokerNodeAddressList) {
                        String msgId = UUID.randomUUID().toString();
                        //拉消息到本地
                        BrokerNettyRemoteClient brokerNettyRemoteClient = this.getBrokerNettyRemoteClientMap().get(brokerNodeAddress);
                        ConsumeMsgReqDTO consumeMsgReqDTO = new ConsumeMsgReqDTO();
                        consumeMsgReqDTO.setMsgId(msgId);
                        consumeMsgReqDTO.setConsumeGroup(consumeGroup);
                        consumeMsgReqDTO.setBatchSize(batchSize);
                        consumeMsgReqDTO.setTopic(pullMsgTopic);
                        TcpMsg pullReqMsg = new TcpMsg(BrokerEventCode.CONSUME_MSG.getCode(), JSON.toJSONBytes(consumeMsgReqDTO));
                        TcpMsg pullMsgResp = brokerNettyRemoteClient.sendSyncMsg(pullReqMsg, msgId);
                        List<ConsumeMsgRespDTO> consumeMsgRespDTOS = null;
                        ConsumeMsgBaseRespDTO consumeMsgBaseRespDTO = JSON.parseObject(pullMsgResp.getBody(), ConsumeMsgBaseRespDTO.class);
                        if (consumeMsgBaseRespDTO != null) {
                            consumeMsgRespDTOS = consumeMsgBaseRespDTO.getConsumeMsgRespDTOList();
                        }
                        boolean brokerHasData = false;
                        //进入业务处理环节
                        if (CollectionUtils.isNotEmpty(consumeMsgRespDTOS)) {
                            for (ConsumeMsgRespDTO consumeMsgRespDTO : consumeMsgRespDTOS) {
                                List<ConsumeMsgCommitLogDTO> commitLogBodyList = consumeMsgRespDTO.getCommitLogContentList();
                                if (CollectionUtils.isEmpty(commitLogBodyList)) {
                                    continue;
                                }
                                List<ConsumeMessage> consumeMessages = new ArrayList<>();
                                for (ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO : commitLogBodyList) {
                                    ConsumeMessage consumeMessage = new ConsumeMessage();
                                    consumeMessage.setConsumeMsgCommitLogDTO(consumeMsgCommitLogDTO);
                                    consumeMessages.add(consumeMessage);
                                }
                                brokerHasData = true;
                                ConsumeResult consumeResult = messageConsumeListener.consume(consumeMessages);
                                //消费成功，发送ack响应
                                if (consumeResult.getConsumeResultStatus() == ConsumeResultStatus.CONSUME_SUCCESS.getCode()) {
                                    this.setQueueId(consumeMsgRespDTO.getQueueId());
                                    String ackMsgId = UUID.randomUUID().toString();
                                    ConsumeMsgAckReqDTO consumeMsgAckReqDTO = new ConsumeMsgAckReqDTO();
                                    consumeMsgAckReqDTO.setAckCount(this.getBatchSize());
                                    consumeMsgAckReqDTO.setConsumeGroup(this.getConsumeGroup());
                                    consumeMsgAckReqDTO.setTopic(this.getTopic());
                                    consumeMsgAckReqDTO.setQueueId(consumeMsgRespDTO.getQueueId());
                                    consumeMsgAckReqDTO.setMsgId(ackMsgId);
                                    TcpMsg ackReq = new TcpMsg(BrokerEventCode.CONSUME_SUCCESS_MSG.getCode(), JSON.toJSONBytes(consumeMsgAckReqDTO));
                                    TcpMsg ackResponse = brokerNettyRemoteClient.sendSyncMsg(ackReq, ackMsgId);
                                    ConsumeMsgAckRespDTO consumeMsgAckRespDTO = JSON.parseObject(ackResponse.getBody(), ConsumeMsgAckRespDTO.class);
                                    if (AckStatus.SUCCESS.getCode() == consumeMsgAckRespDTO.getAckStatus()) {
                                        logger.info("consume ack success!");
                                    } else {
                                        logger.error("consume ack fail!");
                                    }
                                }
                                //消费失败，回应ack，然后丢入重试队列中
                                else if (consumeResult.getConsumeResultStatus() == ConsumeResultStatus.CONSUME_LATER.getCode()) {
                                    this.setQueueId(consumeMsgRespDTO.getQueueId());
                                    ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO = new ConsumeMsgRetryReqDTO();
                                    List<ConsumeMsgRetryReqDetailDTO> consumeMsgRetryReqDetailDTOList = new ArrayList<>();
                                    for (int i = 0; i < commitLogBodyList.size(); i++) {
                                        ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO = commitLogBodyList.get(i);
                                        ConsumeMsgRetryReqDetailDTO consumeMsgRetryReqDetailDTO = new ConsumeMsgRetryReqDetailDTO();
                                        consumeMsgRetryReqDetailDTO.setConsumerGroup(this.getConsumeGroup());
                                        consumeMsgRetryReqDetailDTO.setTopic(this.getTopic());
                                        consumeMsgRetryReqDetailDTO.setQueueId(consumeMsgRespDTO.getQueueId());
                                        consumeMsgRetryReqDetailDTO.setCommitLogOffset(consumeMsgCommitLogDTO.getCommitLogOffset());
                                        consumeMsgRetryReqDetailDTO.setCommitLogMsgLength(consumeMsgCommitLogDTO.getCommitLogSize());
                                        consumeMsgRetryReqDetailDTO.setRetryTime(consumeMsgCommitLogDTO.getRetryTimes() + 1);
                                        consumeMsgRetryReqDetailDTO.setCommitLogName(consumeMsgCommitLogDTO.getFileName());
                                        consumeMsgRetryReqDetailDTOList.add(consumeMsgRetryReqDetailDTO);
                                        logger.info("重试次数:{}" , consumeMsgCommitLogDTO.getRetryTimes());
                                    }
                                    consumeMsgRetryReqDTO.setConsumeMsgRetryReqDetailDTOList(consumeMsgRetryReqDetailDTOList);
                                    consumeMsgRetryReqDTO.setMsgId(UUID.randomUUID().toString());
                                    TcpMsg tcpMsg = new TcpMsg(BrokerEventCode.CONSUME_LATER_MSG.getCode(), JSON.toJSONBytes(consumeMsgRetryReqDTO));
                                    TcpMsg resp = brokerNettyRemoteClient.sendSyncMsg(tcpMsg,consumeMsgRetryReqDTO.getMsgId());
                                    logger.info("consume later resp:{}", JSON.toJSONString(resp));
                                }
                            }
                        }
                        if (brokerHasData) {
                            TimeUnit.MILLISECONDS.sleep(EACH_BATCH_PULL_MSG_INTER);
                        } else {
                            TimeUnit.MILLISECONDS.sleep(EACH_BATCH_PULL_MSG_INTER_WHEN_NO_MSG);
                        }
                    }
                } catch (Exception e) {
                    logger.error("consume has error:", e);
                    try {
                        TimeUnit.MILLISECONDS.sleep(EACH_BATCH_PULL_MSG_INTER_WHEN_NO_MSG);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        });
        consumeTask.setName("consume-msg-task");
        consumeTask.start();
    }

    /**
     * 连接broker程序
     */
    private void connectBroker() {
        List<String> brokerAddressList = new ArrayList<>();
        if("single".equals(this.getBrokerRole())) {
            AssertUtils.isNotEmpty(this.getBrokerAddressList(), "broker地址不能为空");
            brokerAddressList = this.getBrokerAddressList();
        } else if("slave".equals(this.getBrokerRole())) {
            AssertUtils.isNotEmpty(this.getSlaveAddressList(), "broker地址不能为空");
            brokerAddressList = this.getSlaveAddressList();
        } else if("master".equals(this.getBrokerRole())) {
            AssertUtils.isNotEmpty(this.getMasterAddressList(), "broker地址不能为空");
            brokerAddressList = this.getMasterAddressList();
        }
        //判断之前是否有链接过目标地址，以及链接是否正常，如果链接正常则没必要重新链接，避免无意义的通讯中断情况发生
        List<BrokerNettyRemoteClient> newBrokerNettyRemoteClientList = new ArrayList<>();
        for (String brokerIp : brokerAddressList) {
            BrokerNettyRemoteClient brokerNettyRemoteClient = this.getBrokerNettyRemoteClientMap().get(brokerIp);
            if(brokerNettyRemoteClient == null) {
                //之前没有链接过，需要额外链接接入
                String[] brokerAddressArr = brokerIp.split(":");
                BrokerNettyRemoteClient newBrokerNettyRemoteClient = new BrokerNettyRemoteClient(brokerAddressArr[0],
                        Integer.valueOf(brokerAddressArr[1]));
                newBrokerNettyRemoteClient.buildConnection(new BrokerRemoteRespHandler(new EventBus("consumer-client-eventbus")));
                //新的链接通道建立
                newBrokerNettyRemoteClientList.add(newBrokerNettyRemoteClient);
                continue;
            }
            //老链接依然需要使用，而且链接顺畅，则继续使用
            if(brokerNettyRemoteClient.isChannelActive()) {
                newBrokerNettyRemoteClientList.add(brokerNettyRemoteClient);
                continue;
            }
            //老链接通讯失败，重连尝试
            String[] brokerAddressArr = brokerIp.split(":");
            BrokerNettyRemoteClient newBrokerNettyRemoteClient = new BrokerNettyRemoteClient(brokerAddressArr[0],
                    Integer.valueOf(brokerAddressArr[1]));
            newBrokerNettyRemoteClient.buildConnection(new BrokerRemoteRespHandler(new EventBus("consumer-client-eventbus")));
            //新的链接通道建立
            newBrokerNettyRemoteClientList.add(newBrokerNettyRemoteClient);
        }
        //需要被关闭的链接过滤出来，进行优雅暂停，然后切换使用新的链接
        List<String> finalBrokerAddressList = brokerAddressList;
        List<String> needRemoveBrokerId = this.getBrokerNettyRemoteClientMap().keySet().stream().filter(reqId-> !finalBrokerAddressList.contains(reqId)).collect(Collectors.toList());
        for (String brokerReqId : needRemoveBrokerId) {
            getBrokerNettyRemoteClientMap().get(brokerReqId).close();
            this.getBrokerNettyRemoteClientMap().remove(brokerReqId);
        }
        this.setBrokerNettyRemoteClientMap(newBrokerNettyRemoteClientList.stream().collect(Collectors.toMap(BrokerNettyRemoteClient::getBrokerReqId,item->item)));
    }


    /**
     * 开启注册
     *
     * @return
     */
    private boolean doRegistry() {
        String registryMsgId = UUID.randomUUID().toString();
        ServiceRegistryReqDTO serviceRegistryReqDTO = new ServiceRegistryReqDTO();
        serviceRegistryReqDTO.setMsgId(registryMsgId);
        serviceRegistryReqDTO.setUser(nsUser);
        serviceRegistryReqDTO.setPassword(nsPwd);
        serviceRegistryReqDTO.setRegistryType(RegistryTypeEnum.CONSUMER.getCode());
        TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.REGISTRY.getCode(), JSON.toJSONBytes(serviceRegistryReqDTO));
        TcpMsg registryResponse = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg, registryMsgId);
        if (NameServerResponseCode.REGISTRY_SUCCESS.getCode() == registryResponse.getCode()) {
            return true;
        } else {
            logger.error("注册账号失败");
            return false;
        }
    }

    /**
     * 启动心跳任务
     */
    private void startHeartBeatTask() {
        Thread heartBeatTask = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(3);
                        logger.info("执行心跳数据发送");
                        String heartBeatMsgId = UUID.randomUUID().toString();
                        HeartBeatDTO heartBeatDTO = new HeartBeatDTO();
                        heartBeatDTO.setMsgId(heartBeatMsgId);
                        TcpMsg heartBeatResponse = nameServerNettyRemoteClient.sendSyncMsg(new TcpMsg(NameServerEventCode.HEART_BEAT.getCode(),
                                JSON.toJSONBytes(heartBeatDTO)), heartBeatMsgId);
                        logger.info("heart beat response data is :{}", JSON.parseObject(heartBeatResponse.getBody()));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "heart-beat-task");
        heartBeatTask.start();
    }

    /**
     * 拉broker地址
     * <p>
     * 主从架构 -》从节点数据 / 主节点数据（两套ip都应该保存下来）
     */
    public void fetchBrokerAddress() {
        String fetchBrokerAddressMsgId = UUID.randomUUID().toString();
        PullBrokerIpDTO pullBrokerIpDTO = new PullBrokerIpDTO();
        if (getBrokerClusterGroup() != null) {
            this.setBrokerRole("master");
            pullBrokerIpDTO.setBrokerClusterGroup(brokerClusterGroup);
        }
        pullBrokerIpDTO.setRole(getBrokerRole());
        pullBrokerIpDTO.setMsgId(fetchBrokerAddressMsgId);
        TcpMsg heartBeatResponse = nameServerNettyRemoteClient.sendSyncMsg(new TcpMsg(NameServerEventCode.PULL_BROKER_IP_LIST.getCode(),
                JSON.toJSONBytes(pullBrokerIpDTO)), fetchBrokerAddressMsgId);
        //获取broker节点ip地址，并且缓存起来，可能由多个master-broker角色
        PullBrokerIpRespDTO pullBrokerIpRespDTO = JSON.parseObject(heartBeatResponse.getBody(), PullBrokerIpRespDTO.class);
        this.setBrokerAddressList(pullBrokerIpRespDTO.getAddressList());
        this.setMasterAddressList(pullBrokerIpRespDTO.getMasterAddressList());
        this.setSlaveAddressList(pullBrokerIpRespDTO.getSlaveAddressList());
        logger.info("fetch broker address:{},master:{},slave:{}", this.getBrokerAddressList(),this.getMasterAddressList(),this.getSlaveAddressList());
        this.connectBroker();
    }


    /**
     * 开启一个定时拉broker地址的任务
     */
    public void startRefreshBrokerAddressJob() {
        Thread refreshBrokerAddressJob = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(3);
                        fetchBrokerAddress();
                    } catch (Exception e) {
                        logger.error("refresh broker address job error:", e);
                    }
                }
            }
        });
        refreshBrokerAddressJob.setName("refresh-broker-address-job");
        refreshBrokerAddressJob.start();
    }

    public String getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = brokerRole;
    }

    public MessageConsumeListener getMessageConsumeListener() {
        return messageConsumeListener;
    }

    public void setMessageConsumeListener(MessageConsumeListener messageConsumeListener) {
        this.messageConsumeListener = messageConsumeListener;
    }

    public NameServerNettyRemoteClient getNameServerNettyRemoteClient() {
        return nameServerNettyRemoteClient;
    }

    public void setNameServerNettyRemoteClient(NameServerNettyRemoteClient nameServerNettyRemoteClient) {
        this.nameServerNettyRemoteClient = nameServerNettyRemoteClient;
    }

    public String getNsIp() {
        return nsIp;
    }

    public void setNsIp(String nsIp) {
        this.nsIp = nsIp;
    }

    public Integer getNsPort() {
        return nsPort;
    }

    public void setNsPort(Integer nsPort) {
        this.nsPort = nsPort;
    }

    public String getNsUser() {
        return nsUser;
    }

    public void setNsUser(String nsUser) {
        this.nsUser = nsUser;
    }

    public String getNsPwd() {
        return nsPwd;
    }

    public void setNsPwd(String nsPwd) {
        this.nsPwd = nsPwd;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumeGroup() {
        return consumeGroup;
    }

    public void setConsumeGroup(String consumeGroup) {
        this.consumeGroup = consumeGroup;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getBrokerClusterGroup() {
        return brokerClusterGroup;
    }

    public void setBrokerClusterGroup(String brokerClusterGroup) {
        this.brokerClusterGroup = brokerClusterGroup;
    }

    public List<String> getBrokerAddressList() {
        return brokerAddressList;
    }

    public void setBrokerAddressList(List<String> brokerAddressList) {
        this.brokerAddressList = brokerAddressList;
    }

    public Map<String, BrokerNettyRemoteClient> getBrokerNettyRemoteClientMap() {
        return brokerNettyRemoteClientMap;
    }

    public void setBrokerNettyRemoteClientMap(Map<String, BrokerNettyRemoteClient> brokerNettyRemoteClientMap) {
        this.brokerNettyRemoteClientMap = brokerNettyRemoteClientMap;
    }

    public List<String> getMasterAddressList() {
        return masterAddressList;
    }

    public void setMasterAddressList(List<String> masterAddressList) {
        this.masterAddressList = masterAddressList;
    }

    public List<String> getSlaveAddressList() {
        return slaveAddressList;
    }

    public void setSlaveAddressList(List<String> slaveAddressList) {
        this.slaveAddressList = slaveAddressList;
    }
}
