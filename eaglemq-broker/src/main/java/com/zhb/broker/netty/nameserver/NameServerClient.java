package com.zhb.broker.netty.nameserver;

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.StringUtil;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.config.GlobalProperties;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.constants.TcpConstants;
import com.zhb.common.dto.PullBrokerIpDTO;
import com.zhb.common.dto.PullBrokerIpRespDTO;
import com.zhb.common.dto.ServiceRegistryReqDTO;
import com.zhb.common.enums.*;
import com.zhb.common.remote.NameServerNettyRemoteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author idea
 * @Date: Created in 08:32 2024/5/7
 * @Description 服务与nameserver服务端创建长链接，支持链接创建，支持重试机制
 */
public class NameServerClient {

    private final Logger logger = LoggerFactory.getLogger(NameServerClient.class);

    private EventLoopGroup clientGroup = new NioEventLoopGroup();
    private Bootstrap bootstrap = new Bootstrap();
    private Channel channel;
    private String DEFAULT_NAMESERVER_IP = "127.0.0.1";

    private NameServerNettyRemoteClient nameServerNettyRemoteClient;

    public NameServerNettyRemoteClient getNameServerNettyRemoteClient() {
        return nameServerNettyRemoteClient;
    }

    /**
     * 初始化链接
     */
    public void initConnection() {
        String ip = CommonCache.getGlobalProperties().getNameserverIp();
        Integer port = CommonCache.getGlobalProperties().getNameserverPort();
        if (StringUtil.isNullOrEmpty(ip) || port == null || port < 0) {
            throw new RuntimeException("error port or ip");
        }
        nameServerNettyRemoteClient = new NameServerNettyRemoteClient(ip,port);
        nameServerNettyRemoteClient.buildConnection();
    }


    /**
     * 发送节点注册消息
     */
    public void sendRegistryMsg() {
        ServiceRegistryReqDTO registryDTO = new ServiceRegistryReqDTO();
        try {
            Map<String,Object> attrs = new HashMap<>();
            GlobalProperties globalProperties = CommonCache.getGlobalProperties();
            String clusterMode = globalProperties.getBrokerClusterMode();
            if(StringUtil.isNullOrEmpty(clusterMode)) {
                attrs.put("role","single");
            } else if(BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(clusterMode)){
                //注册模式是集群架构
                BrokerRegistryRoleEnum brokerRegistryEnum = BrokerRegistryRoleEnum.of(globalProperties.getBrokerClusterRole());
                attrs.put("role",brokerRegistryEnum.getCode());
                attrs.put("group",globalProperties.getBrokerClusterGroup());
            }
            //broker是主从架构,producer（主发送数据）,consumer(主，从拉数据)Ï
            registryDTO.setIp(Inet4Address.getLocalHost().getHostAddress());
            registryDTO.setPort(globalProperties.getBrokerPort());
            registryDTO.setUser(globalProperties.getNameserverUser());
            registryDTO.setPassword(globalProperties.getNameserverPassword());
            registryDTO.setRegistryType(RegistryTypeEnum.BROKER.getCode());
            registryDTO.setAttrs(attrs);
            registryDTO.setMsgId(UUID.randomUUID().toString());
            byte[] body = JSON.toJSONBytes(registryDTO);

            //发送注册数据给nameserver
            TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.REGISTRY.getCode(), body);
            TcpMsg registryResponseMsg = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg,registryDTO.getMsgId());
            if(NameServerResponseCode.REGISTRY_SUCCESS.getCode() == registryResponseMsg.getCode()) {
                //注册成功！
                //开启一个定时任务，上报心跳数据给到nameserver
                logger.info("注册成功，开启心跳任务");
                CommonCache.getHeartBeatTaskManager().startTask();
            } else if (NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode() == registryResponseMsg.getCode()) {
                //验证失败，抛出异常
                throw new RuntimeException("error nameserver user or password");
            } else if (NameServerResponseCode.HEART_BEAT_SUCCESS.getCode() == registryResponseMsg.getCode()) {
                logger.info("收到nameserver心跳回应ack");
            }
            logger.info("registry resp is:{}",JSON.toJSONString(registryResponseMsg.getBody()));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 获取broker主节点的地址
     */
    public String queryBrokerMasterAddress() {
        String clusterMode = CommonCache.getGlobalProperties().getBrokerClusterMode();
        if(!BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(clusterMode)) {
            return null;
        }
        PullBrokerIpDTO pullBrokerIpDTO = new PullBrokerIpDTO();
        pullBrokerIpDTO.setBrokerClusterGroup(CommonCache.getGlobalProperties().getBrokerClusterGroup());
        pullBrokerIpDTO.setRole(BrokerRegistryRoleEnum.MASTER.getCode());
        pullBrokerIpDTO.setMsgId(UUID.randomUUID().toString());
        TcpMsg tcpMsg  = new TcpMsg(NameServerEventCode.PULL_BROKER_IP_LIST.getCode(), JSON.toJSONBytes(pullBrokerIpDTO));
        TcpMsg pullBrokerIpResponse = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg,pullBrokerIpDTO.getMsgId());
        PullBrokerIpRespDTO pullBrokerIpRespDTO = JSON.parseObject(pullBrokerIpResponse.getBody(),PullBrokerIpRespDTO.class);
        return pullBrokerIpRespDTO.getMasterAddressList().get(0);
    }



}
