package com.zhb.client.producer;

import com.alibaba.fastjson.JSON;
import com.zhb.client.common.CommonCache;
import com.zhb.client.netty.BrokerRemoteRespHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.*;
import com.zhb.common.enums.*;
import com.zhb.common.event.EventBus;
import com.zhb.common.remote.BrokerNettyRemoteClient;
import com.zhb.common.remote.NameServerNettyRemoteClient;
import com.zhb.common.transaction.TransactionListener;
import com.zhb.common.utils.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author idea
 * @Date: Created in 16:00 2024/6/11
 * @Description 提供基础的mq发送接口
 */
public class DefaultProducerImpl implements Producer {

    private static Logger logger = LoggerFactory.getLogger(DefaultProducerImpl.class);

    //连接nameserver，给nameserver发送心跳，拉取broker地址
    //与broker建立连接，发送数据给到broker节点

    private String nsIp;
    private Integer nsPort;
    private String nsUser;
    private String nsPwd;
    private String brokerClusterGroup;
    private String brokerRole = "single";
    private List<String> brokerAddressList; //broker地址会有多个？broker节点会有多个，水平扩展的效果，水平扩展（存储内容会增加，承载压力也会大大增加，节点的选择问题）
    private List<String> masterAddressList;
    private TransactionListener transactionListener;
    private String producerId;
    private NameServerNettyRemoteClient nameServerNettyRemoteClient;
    private Map<String, BrokerNettyRemoteClient> brokerNettyRemoteClientMap = new ConcurrentHashMap<>();

    public String getProducerId() {
        return producerId;
    }

    public void setProducerId(String producerId) {
        this.producerId = producerId;
    }

    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;
    }

    public String getBrokerClusterGroup() {
        return brokerClusterGroup;
    }

    public void setBrokerClusterGroup(String brokerClusterGroup) {
        this.brokerClusterGroup = brokerClusterGroup;
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

    public List<String> getBrokerAddressList() {
        return brokerAddressList;
    }

    public Map<String, BrokerNettyRemoteClient> getBrokerNettyRemoteClientMap() {
        return brokerNettyRemoteClientMap;
    }

    public void setBrokerNettyRemoteClientMap(Map<String, BrokerNettyRemoteClient> brokerNettyRemoteClientMap) {
        this.brokerNettyRemoteClientMap = brokerNettyRemoteClientMap;
    }

    public void setBrokerAddressList(List<String> brokerAddressList) {
        this.brokerAddressList = brokerAddressList;
    }

    public void start() {
        nameServerNettyRemoteClient = new NameServerNettyRemoteClient(nsIp, nsPort);
        nameServerNettyRemoteClient.buildConnection();
        boolean isRegistrySuccess = this.doRegistry();
        this.setProducerId(UUID.randomUUID().toString());
        if (this.getTransactionListener() != null) {
            CommonCache.getTransactionListenerMap().put(this.getProducerId(), this.getTransactionListener());
        }
        if (isRegistrySuccess) {
            this.startHeartBeatTask();
            this.fetchBrokerAddress();
            this.startRefreshBrokerAddressJob();
        }
    }


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
        serviceRegistryReqDTO.setRegistryType(RegistryTypeEnum.PRODUCER.getCode());
        TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.REGISTRY.getCode(), JSON.toJSONBytes(serviceRegistryReqDTO));
        TcpMsg registryResponse = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg, registryMsgId);
        if (NameServerResponseCode.REGISTRY_SUCCESS.getCode() == registryResponse.getCode()) {
            return true;
        } else {
            logger.error("注册账号失败");
            return false;
        }
    }

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
        pullBrokerIpDTO.setRole(this.getBrokerRole());
        pullBrokerIpDTO.setMsgId(fetchBrokerAddressMsgId);
        TcpMsg heartBeatResponse = nameServerNettyRemoteClient.sendSyncMsg(new TcpMsg(NameServerEventCode.PULL_BROKER_IP_LIST.getCode(),
                JSON.toJSONBytes(pullBrokerIpDTO)), fetchBrokerAddressMsgId);
        //获取broker节点ip地址，并且缓存起来，可能由多个master-broker角色
        PullBrokerIpRespDTO pullBrokerIpRespDTO = JSON.parseObject(heartBeatResponse.getBody(), PullBrokerIpRespDTO.class);
        this.setBrokerAddressList(pullBrokerIpRespDTO.getAddressList());
        this.setMasterAddressList(pullBrokerIpRespDTO.getMasterAddressList());
        logger.info("fetch broker address:{},master:{},slave:{}", this.getBrokerAddressList(), this.getMasterAddressList());
        this.connectBroker();
    }

    /**
     * 连接broker程序
     */
    private void connectBroker() {
        List<String> brokerAddressList = new ArrayList<>();
        if ("single".equals(this.getBrokerRole())) {
            AssertUtils.isNotEmpty(this.getBrokerAddressList(), "broker地址不能为空");
            brokerAddressList = this.getBrokerAddressList();
        } else if ("master".equals(this.getBrokerRole())) {
            AssertUtils.isNotEmpty(this.getMasterAddressList(), "broker地址不能为空");
            brokerAddressList = this.getMasterAddressList();
        }
        //判断之前是否有链接过目标地址，以及链接是否正常，如果链接正常则没必要重新链接，避免无意义的通讯中断情况发生
        List<BrokerNettyRemoteClient> newBrokerNettyRemoteClientList = new ArrayList<>();
        for (String brokerIp : brokerAddressList) {
            BrokerNettyRemoteClient brokerNettyRemoteClient = this.getBrokerNettyRemoteClientMap().get(brokerIp);
            if (brokerNettyRemoteClient == null) {
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
            if (brokerNettyRemoteClient.isChannelActive()) {
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
        List<String> needRemoveBrokerId = this.getBrokerNettyRemoteClientMap().keySet().stream().filter(reqId -> !finalBrokerAddressList.contains(reqId)).collect(Collectors.toList());
        for (String brokerReqId : needRemoveBrokerId) {
            getBrokerNettyRemoteClientMap().get(brokerReqId).close();
            this.getBrokerNettyRemoteClientMap().remove(brokerReqId);
        }
        this.setBrokerNettyRemoteClientMap(newBrokerNettyRemoteClientList.stream().collect(Collectors.toMap(BrokerNettyRemoteClient::getBrokerReqId, item -> item)));
    }


    @Override
    public SendResult send(MessageDTO messageDTO) {
        //路由的关系(topic->定位到具体的broker实例的ip)
        BrokerNettyRemoteClient remoteClient = this.getBrokerNettyRemoteClient();
        String msgId = UUID.randomUUID().toString();
        messageDTO.setMsgId(msgId);
        messageDTO.setSendWay(MessageSendWay.SYNC.getCode());
        TcpMsg tcpMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
        TcpMsg responseMsg = remoteClient.sendSyncMsg(tcpMsg, msgId);
        SendMessageToBrokerResponseDTO sendMessageToBrokerResponseDTO = JSON.parseObject(responseMsg.getBody(), SendMessageToBrokerResponseDTO.class);
        int responseStatus = sendMessageToBrokerResponseDTO.getStatus();
        SendResult sendResult = new SendResult();
        if (responseStatus == SendMessageToBrokerResponseStatus.SUCCESS.getCode()) {
            sendResult.setSendStatus(SendStatus.SUCCESS);
        } else if (responseStatus == SendMessageToBrokerResponseStatus.FAIL.getCode()) {
            sendResult.setSendStatus(SendStatus.FAIL);
            logger.error("send mq fail:{}", sendMessageToBrokerResponseDTO.getDesc());
        }
        return sendResult;
    }

    @Override
    public void sendAsync(MessageDTO messageDTO) {
        BrokerNettyRemoteClient remoteClient = this.getBrokerNettyRemoteClient();
        messageDTO.setSendWay(MessageSendWay.ASYNC.getCode());
        TcpMsg tcpMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
        remoteClient.sendAsyncMsg(tcpMsg);
    }

    @Override
    public SendResult sendTxMessage(MessageDTO messageDTO) {
        AssertUtils.isNotNull(messageDTO, "messageDTO is null");
        AssertUtils.isNotNull(transactionListener, "transactionListener is null");
        String msgId = UUID.randomUUID().toString();
        BrokerNettyRemoteClient remoteClient = this.getBrokerNettyRemoteClient();
        messageDTO.setTxFlag(TxMessageFlagEnum.HALF_MSG.getCode());
        messageDTO.setMsgId(msgId);
        messageDTO.setProducerId(this.getProducerId());
        messageDTO.setSendWay(MessageSendWay.SYNC.getCode());
        TcpMsg tcpMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
        TcpMsg responseMsg = remoteClient.sendSyncMsg(tcpMsg, msgId);
        boolean isHalfMsgSendSuccess = (responseMsg != null && responseMsg.getCode() == BrokerResponseCode.HALF_MSG_SEND_SUCCESS.getCode());
        if (!isHalfMsgSendSuccess) {
            throw new RuntimeException("half msg send fail");
        }
        //本地事务代码的执行位置
        LocalTransactionState localTransactionState = transactionListener.executeLocalTransaction(messageDTO);
        AssertUtils.isNotNull(localTransactionState != null, "localTransactionState is null");
        messageDTO.setLocalTxState(localTransactionState.getCode());

        if (LocalTransactionState.COMMIT.equals(localTransactionState)) {
            messageDTO.setTxFlag(TxMessageFlagEnum.REMAIN_HALF_ACK.getCode());
            TcpMsg remainHalfAckMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
            TcpMsg remainHalfAckResp = remoteClient.sendSyncMsg(remainHalfAckMsg, msgId);
            logger.info("sendTxMessage remainHalfAckResp :{}", remainHalfAckResp);
        } else if (LocalTransactionState.ROLLBACK.equals(localTransactionState)) {
            //应该要通知到broker本地事务消息执行失败这个行为，不然的话broker会一直回调客户端查询状态，这里会有额外的性能损耗
            messageDTO.setTxFlag(TxMessageFlagEnum.REMAIN_HALF_ACK.getCode());
            TcpMsg rollbackMsg = new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO));
            TcpMsg rollbackAckResp = remoteClient.sendSyncMsg(rollbackMsg, msgId);
            logger.info("sendTxMessage rollbackAckResp :{}", rollbackAckResp);
        } else if (LocalTransactionState.UNKNOW.equals(localTransactionState)) {
            //等待broker回调查询进行状态判断
        }
        //本地事务执行环节
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SUCCESS);
        return sendResult;
    }

    private BrokerNettyRemoteClient getBrokerNettyRemoteClient() {
        BrokerNettyRemoteClient remoteClient = this.getBrokerNettyRemoteClientMap().values().stream().collect(Collectors.toList()).get(0);
        return remoteClient;
    }

    public List<String> getMasterAddressList() {
        return masterAddressList;
    }

    public void setMasterAddressList(List<String> masterAddressList) {
        this.masterAddressList = masterAddressList;
    }

    public NameServerNettyRemoteClient getNameServerNettyRemoteClient() {
        return nameServerNettyRemoteClient;
    }

    public void setNameServerNettyRemoteClient(NameServerNettyRemoteClient nameServerNettyRemoteClient) {
        this.nameServerNettyRemoteClient = nameServerNettyRemoteClient;
    }

    public String getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = brokerRole;
    }
}
