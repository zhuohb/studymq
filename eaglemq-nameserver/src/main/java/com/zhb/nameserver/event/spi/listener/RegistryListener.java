package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.enums.ReplicationModeEnum;
import com.zhb.nameserver.enums.ReplicationMsgTypeEnum;
import com.zhb.nameserver.event.model.RegistryEvent;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.nameserver.utils.NameserverUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.ServiceRegistryRespDTO;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @Author idea
 * @Date: Created in 14:44 2024/5/4
 * @Description
 */
public class RegistryListener implements Listener<RegistryEvent> {

	private final Logger logger = LoggerFactory.getLogger(RegistryListener.class);

	@Override
	public void onReceive(RegistryEvent event) throws IllegalAccessException {
		//安全认证
		boolean isVerify = NameserverUtils.isVerify(event.getUser(), event.getPassword());
		ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();
		if (!isVerify) {
			ServiceRegistryRespDTO registryRespDTO = new ServiceRegistryRespDTO();
			registryRespDTO.setMsgId(event.getMsgId());
			TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
				JSON.toJSONBytes(registryRespDTO));
			channelHandlerContext.writeAndFlush(tcpMsg);
			channelHandlerContext.close();
			throw new IllegalAccessException("error account to connected!");
		}
		logger.info("注册事件接收:{}", JSON.toJSONString(event));
		channelHandlerContext.attr(AttributeKey.valueOf("reqId")).set(event.getIp() + ":" + event.getPort());
		ServiceInstance serviceInstance = new ServiceInstance();
		serviceInstance.setIp(event.getIp());
		serviceInstance.setPort(event.getPort());
		serviceInstance.setRegistryType(event.getRegistryType());
		serviceInstance.setAttrs(event.getAttrs());
		serviceInstance.setFirstRegistryTime(System.currentTimeMillis());
		CommonCache.getServiceInstanceManager().put(serviceInstance);
		ReplicationModeEnum replicationModeEnum = ReplicationModeEnum.of(CommonCache.getNameserverProperties().getReplicationMode());
		if (replicationModeEnum == null) {
			//单机架构，直接返回注册成功
			String msgId = event.getMsgId();
			ServiceRegistryRespDTO serviceRegistryRespDTO = new ServiceRegistryRespDTO();
			serviceRegistryRespDTO.setMsgId(msgId);
			TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.REGISTRY_SUCCESS.getCode(),
				JSON.toJSONBytes(serviceRegistryRespDTO));
			channelHandlerContext.writeAndFlush(tcpMsg);
			return;
		}
		//如果当前是主从复制模式，而且当前角色是主节点，那么就往队列里面防元素
		ReplicationMsgEvent replicationMsgEvent = new ReplicationMsgEvent();
		replicationMsgEvent.setServiceInstance(serviceInstance);
		replicationMsgEvent.setMsgId(UUID.randomUUID().toString());
		replicationMsgEvent.setChannelHandlerContext(event.getChannelHandlerContext());
		replicationMsgEvent.setType(ReplicationMsgTypeEnum.REGISTRY.getCode());
		CommonCache.getReplicationMsgQueueManager().put(replicationMsgEvent);
		//同步给到从节点，比较严谨的同步，binlog类型，对于数据的顺序性要求很高了
		//可能是无顺序的状态
		//把同步的数据塞入一条队列当中，专门有一条线程从队列当中提取数据，同步给各个从节点
	}

}
