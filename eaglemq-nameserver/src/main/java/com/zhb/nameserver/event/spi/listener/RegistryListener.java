package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.ServiceRegistryRespDTO;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.enums.ReplicationModeEnum;
import com.zhb.nameserver.enums.ReplicationMsgTypeEnum;
import com.zhb.nameserver.event.model.RegistryEvent;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.nameserver.utils.NameserverUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * 服务注册监听器
 * <p>
 * 负责处理服务实例的注册请求，完成身份验证、实例信息保存和数据同步等关键步骤
 * 是服务发现机制的核心组件，确保服务实例能够被正确注册到NameServer中
 * <p>
 * 主要功能：
 * 1. 对注册服务进行安全认证，验证用户凭证
 * 2. 将服务实例信息存储到服务实例管理器中
 * 3. 根据系统配置的复制模式处理数据同步
 * 4. 支持单机模式直接注册和主从模式数据复制
 * <p>
 * 使用场景：
 * - 服务启动时向NameServer注册自身信息
 * - Broker节点注册到NameServer以便被客户端发现
 * - 服务实例状态变更时更新注册信息
 */
@Slf4j
public class RegistryListener implements Listener<RegistryEvent> {

	/**
	 * 处理服务注册事件
	 * <p>
	 * 接收并处理服务实例的注册请求，包括认证、数据存储和复制同步等步骤，
	 * 根据系统配置的复制模式决定如何处理数据同步逻辑。
	 * <p>
	 * 处理流程：
	 * 1. 验证注册请求中的用户身份信息
	 * 2. 提取服务实例信息并创建ServiceInstance对象
	 * 3. 将服务实例保存到全局缓存中
	 * 4. 根据复制模式决定后续处理：
	 * - 单机模式：直接返回注册成功响应
	 * - 主从复制模式：将注册事件加入复制队列以同步给从节点
	 * 5. 返回适当的响应给客户端
	 *
	 * @param event 包含注册信息的事件对象
	 * @throws IllegalAccessException 认证失败时抛出异常
	 */
	@Override
	public void onReceive(RegistryEvent event) throws IllegalAccessException {
		// 安全认证，验证用户凭证
		boolean isVerify = NameserverUtils.isVerify(event.getUser(), event.getPassword());
		ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();
		if (!isVerify) {
			// 认证失败，返回错误响应并关闭连接
			ServiceRegistryRespDTO registryRespDTO = new ServiceRegistryRespDTO();
			registryRespDTO.setMsgId(event.getMsgId());
			TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
				JSON.toJSONBytes(registryRespDTO));
			channelHandlerContext.writeAndFlush(tcpMsg);
			channelHandlerContext.close();
			throw new IllegalAccessException("error account to connected!");
		}
		log.info("注册事件接收:{}", JSON.toJSONString(event));

		// 设置连接通道属性，用于后续标识该连接对应的服务实例
		channelHandlerContext.attr(AttributeKey.valueOf("reqId")).set(event.getIp() + ":" + event.getPort());

		// 创建服务实例对象并设置属性
		ServiceInstance serviceInstance = new ServiceInstance();
		serviceInstance.setIp(event.getIp());
		serviceInstance.setPort(event.getPort());
		serviceInstance.setRegistryType(event.getRegistryType());
		serviceInstance.setAttrs(event.getAttrs());
		serviceInstance.setFirstRegistryTime(System.currentTimeMillis());

		// 存储服务实例到全局缓存
		CommonCache.getServiceInstanceManager().put(serviceInstance);

		// 获取系统配置的复制模式
		ReplicationModeEnum replicationModeEnum = ReplicationModeEnum.of(CommonCache.getNameserverProperties().getReplicationMode());
		if (replicationModeEnum == null) {
			// 单机架构模式，直接返回注册成功响应
			String msgId = event.getMsgId();
			ServiceRegistryRespDTO serviceRegistryRespDTO = new ServiceRegistryRespDTO();
			serviceRegistryRespDTO.setMsgId(msgId);
			TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.REGISTRY_SUCCESS.getCode(),
				JSON.toJSONBytes(serviceRegistryRespDTO));
			channelHandlerContext.writeAndFlush(tcpMsg);
			return;
		}

		// 主从复制模式下，将注册事件加入复制队列
		ReplicationMsgEvent replicationMsgEvent = new ReplicationMsgEvent();
		replicationMsgEvent.setServiceInstance(serviceInstance);
		replicationMsgEvent.setMsgId(UUID.randomUUID().toString());
		replicationMsgEvent.setChannelHandlerContext(event.getChannelHandlerContext());
		replicationMsgEvent.setType(ReplicationMsgTypeEnum.REGISTRY.getCode());

		// 添加到复制消息队列，等待专门的线程处理同步
		CommonCache.getReplicationMsgQueueManager().put(replicationMsgEvent);
		// 异步将数据同步给从节点，保证数据一致性
	}

}
