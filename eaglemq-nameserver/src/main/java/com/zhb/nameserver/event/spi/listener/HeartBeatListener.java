package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.enums.ReplicationMsgTypeEnum;
import com.zhb.nameserver.event.model.HeartBeatEvent;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.store.ServiceInstance;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.HeartBeatDTO;
import com.zhb.common.dto.ServiceRegistryRespDTO;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 心跳包处理监听器
 * <p>
 * 负责处理来自Broker和Client的心跳请求，维护服务实例的活跃状态
 * 心跳机制是服务发现和健康检查的关键组件，确保NameServer能及时感知服务实例状态变化
 * <p>
 * 主要功能：
 * 1. 验证客户端连接的认证状态
 * 2. 更新服务实例的最后心跳时间
 * 3. 回复心跳确认消息
 * 4. 在集群模式下，将心跳事件复制到其他NameServer节点
 * <p>
 * 处理流程：
 * 1. 检查连接的认证状态（reqId属性）
 * 2. 解析服务实例信息（IP、端口）
 * 3. 更新服务实例的心跳时间戳
 * 4. 回复心跳成功响应
 * 5. 将心跳事件放入复制队列，供集群同步
 */
@Slf4j
public class HeartBeatListener implements Listener<HeartBeatEvent> {

	/**
	 * 处理心跳事件
	 * <p>
	 * 当接收到心跳请求时，此方法被调用以验证请求合法性，更新实例状态，
	 * 并在集群模式下传播心跳信息到其他NameServer节点
	 * <p>
	 * 处理步骤：
	 * 1. 验证通信是否有reqId属性，确认连接已认证
	 * 2. 解析reqId获取服务实例的IP和端口信息
	 * 3. 更新服务实例的最后心跳时间为当前时间戳
	 * 4. 回复心跳成功响应消息
	 * 5. 更新服务实例管理器中的实例信息
	 * 6. 创建复制消息事件并发送到复制队列
	 *
	 * @param event 包含心跳信息的事件对象
	 * @throws IllegalAccessException 当客户端未经过有效认证时抛出
	 */
	@Override
	public void onReceive(HeartBeatEvent event) throws IllegalAccessException {
		// 获取通信上下文
		ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();

		// 检查是否已完成认证（验证reqId是否存在）
		Object reqId = channelHandlerContext.attr(AttributeKey.valueOf("reqId")).get();
		if (reqId == null) {
			// 认证失败，构建错误响应并关闭连接
			ServiceRegistryRespDTO serviceRegistryRespDTO = new ServiceRegistryRespDTO();
			serviceRegistryRespDTO.setMsgId(event.getMsgId());
			TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
				JSON.toJSONBytes(serviceRegistryRespDTO));
			channelHandlerContext.writeAndFlush(tcpMsg);
			channelHandlerContext.close();
			throw new IllegalAccessException("error account to connected!");
		}

		log.info("接收到心跳数据：{}", JSON.toJSONString(event));

		// 解析reqId（格式为"IP:端口"）获��服务实例信息
		String reqIdStr = (String) reqId;
		String[] reqInfoStrArr = reqIdStr.split(":");
		long currentTimestamp = System.currentTimeMillis();

		// 更新服务实例的心跳时间
		ServiceInstance serviceInstance = new ServiceInstance();
		serviceInstance.setIp(reqInfoStrArr[0]);
		serviceInstance.setPort(Integer.valueOf(reqInfoStrArr[1]));
		serviceInstance.setLastHeartBeatTime(currentTimestamp);

		// 构建心跳响应并回复客户端
		HeartBeatDTO heartBeatDTO = new HeartBeatDTO();
		heartBeatDTO.setMsgId(event.getMsgId());
		channelHandlerContext.writeAndFlush(new TcpMsg(NameServerResponseCode.HEART_BEAT_SUCCESS.getCode(),
			JSON.toJSONBytes(heartBeatDTO)));

		// 更新服务实例管理器中的实例心跳时时间
		CommonCache.getServiceInstanceManager().putIfExist(serviceInstance);

		// 创建复制消息事件，放入复制队列进行集群同步
		ReplicationMsgEvent replicationMsgEvent = new ReplicationMsgEvent();
		replicationMsgEvent.setServiceInstance(serviceInstance);
		replicationMsgEvent.setMsgId(UUID.randomUUID().toString());
		replicationMsgEvent.setChannelHandlerContext(event.getChannelHandlerContext());
		replicationMsgEvent.setType(ReplicationMsgTypeEnum.HEART_BEAT.getCode());
		CommonCache.getReplicationMsgQueueManager().put(replicationMsgEvent);
	}
}
