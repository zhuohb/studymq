package com.zhb.broker.netty.nameserver;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.config.GlobalProperties;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.PullBrokerIpDTO;
import com.zhb.common.dto.PullBrokerIpRespDTO;
import com.zhb.common.dto.ServiceRegistryReqDTO;
import com.zhb.common.enums.*;
import com.zhb.common.remote.NameServerNettyRemoteClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * NameServer客户端类
 * 负责Broker与NameServer服务端之间的通信
 * 提供连接创建、服务注册、心跳维护和集群节点发现等功能
 */
@Slf4j
public class NameServerClient {

	/**
	 * 客户端事件循环组
	 * 用于处理客户端的网络I/O事件
	 */
	private EventLoopGroup clientGroup = new NioEventLoopGroup();

	/**
	 * 客户端启动引导器
	 * 用于配置和创建客户端连接
	 */
	private Bootstrap bootstrap = new Bootstrap();

	/**
	 * 与NameServer的通信通道
	 */
	private Channel channel;

	/**
	 * NameServer默认IP地址
	 * 当配置中未指定时使用此地址
	 */
	private String DEFAULT_NAMESERVER_IP = "127.0.0.1";

	/**
	 * NameServer远程客户端
	 * 封装了与NameServer通信的底层实现
	 * -- GETTER --
	 * 获取NameServer远程客户端实例
	 *
	 * @return NameServer远程客户端对象
	 */
	@Getter
	private NameServerNettyRemoteClient nameServerNettyRemoteClient;

	/**
	 * 初始化与NameServer的连接
	 * 从全局配置中读取NameServer的IP和端口，并建立连接
	 * 如果配置无效则抛出异常
	 */
	public void initConnection() {
		String ip = CommonCache.getGlobalProperties().getNameserverIp();
		Integer port = CommonCache.getGlobalProperties().getNameserverPort();
		if (StringUtil.isNullOrEmpty(ip) || port == null || port < 0) {
			throw new RuntimeException("error port or ip");
		}
		nameServerNettyRemoteClient = new NameServerNettyRemoteClient(ip, port);
		nameServerNettyRemoteClient.buildConnection();
	}

	/**
	 * 发送节点注册消息
	 * 将当前Broker节点的信息注册到NameServer
	 * 包括IP、端口、集群角色和分组等信息
	 * 注册成功后启动心跳任务
	 */
	public void sendRegistryMsg() {
		ServiceRegistryReqDTO registryDTO = new ServiceRegistryReqDTO();
		try {
			// 构建附加属性信息，包括角色和分组
			Map<String, Object> attrs = new HashMap<>();
			GlobalProperties globalProperties = CommonCache.getGlobalProperties();
			String clusterMode = globalProperties.getBrokerClusterMode();

			// 设置节点角色信息
			if (StringUtil.isNullOrEmpty(clusterMode)) {
				// 单机模式
				attrs.put("role", "single");
			} else if (BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(clusterMode)) {
				// 集群模式，设置角色和分组
				BrokerRegistryRoleEnum brokerRegistryEnum = BrokerRegistryRoleEnum.of(globalProperties.getBrokerClusterRole());
				attrs.put("role", brokerRegistryEnum.getCode());
				attrs.put("group", globalProperties.getBrokerClusterGroup());
			}

			// 设置服务注册信息
			registryDTO.setIp(Inet4Address.getLocalHost().getHostAddress());
			registryDTO.setPort(globalProperties.getBrokerPort());
			registryDTO.setUser(globalProperties.getNameserverUser());
			registryDTO.setPassword(globalProperties.getNameserverPassword());
			registryDTO.setRegistryType(RegistryTypeEnum.BROKER.getCode());
			registryDTO.setAttrs(attrs);
			registryDTO.setMsgId(UUID.randomUUID().toString());
			byte[] body = JSON.toJSONBytes(registryDTO);

			// 发送注册数据给nameserver
			TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.REGISTRY.getCode(), body);
			TcpMsg registryResponseMsg = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg, registryDTO.getMsgId());

			// 处理注册响应
			if (NameServerResponseCode.REGISTRY_SUCCESS.getCode() == registryResponseMsg.getCode()) {
				// 注册成功，启动心跳任务
				log.info("注册成功，开启心跳任务");
				CommonCache.getHeartBeatTaskManager().startTask();
			} else if (NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode() == registryResponseMsg.getCode()) {
				// 认证失败，抛出异常
				throw new RuntimeException("error nameserver user or password");
			} else if (NameServerResponseCode.HEART_BEAT_SUCCESS.getCode() == registryResponseMsg.getCode()) {
				log.info("收到nameserver心跳回应ack");
			}
			log.info("registry resp is:{}", JSON.toJSONString(registryResponseMsg.getBody()));
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 查询Broker主节点地址
	 * 在主从集群模式下，从NameServer获取主节点的IP和端口
	 *
	 * @return 主节点地址字符串，格式为"IP:PORT"，非集群模式返回null
	 */
	public String queryBrokerMasterAddress() {
		String clusterMode = CommonCache.getGlobalProperties().getBrokerClusterMode();
		if (!BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(clusterMode)) {
			// 非集群模式，不需要查询主节点
			return null;
		}

		// 构建查询请求
		PullBrokerIpDTO pullBrokerIpDTO = new PullBrokerIpDTO();
		pullBrokerIpDTO.setBrokerClusterGroup(CommonCache.getGlobalProperties().getBrokerClusterGroup());
		pullBrokerIpDTO.setRole(BrokerRegistryRoleEnum.MASTER.getCode());
		pullBrokerIpDTO.setMsgId(UUID.randomUUID().toString());

		// 发送查询请求并获取响应
		TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.PULL_BROKER_IP_LIST.getCode(), JSON.toJSONBytes(pullBrokerIpDTO));
		TcpMsg pullBrokerIpResponse = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg, pullBrokerIpDTO.getMsgId());

		// 解析响应并返回主节点地址
		PullBrokerIpRespDTO pullBrokerIpRespDTO = JSON.parseObject(pullBrokerIpResponse.getBody(), PullBrokerIpRespDTO.class);
		return pullBrokerIpRespDTO.getMasterAddressList().get(0);
	}
}
