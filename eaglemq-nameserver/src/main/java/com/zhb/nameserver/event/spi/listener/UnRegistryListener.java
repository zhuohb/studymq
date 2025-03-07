package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.UnRegistryEvent;
import com.zhb.nameserver.store.ServiceInstance;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 服务下线监听器
 * <p>
 * 负责处理服务实例的下线请求，实现服务注册表的动态更新和集群主从切换管理
 * 是服务发现机制中保证服务注册表实时性的关键组件，处理集群模式下的故障转移
 * <p>
 * 主要功能：
 * 1. 处理服务实例的正常下线请求
 * 2. 验证请求的合法性和权限
 * 3. 从服务注册表中移除下线实例
 * 4. 在集群模式下管理主从节点关系和故障转移
 * 5. 在主节点下线时选举最新版本的从节点作为新主节点
 * <p>
 * 使用场景：
 * - 服务实例正常下线时的注册表更新
 * - Broker集群模式下主节点故障的自动转移
 * - 集群重组和主从关系重新配置
 * - 系统停机维护时的服务优雅下线
 */
public class UnRegistryListener implements Listener<UnRegistryEvent> {

	private final Logger logger = LoggerFactory.getLogger(UnRegistryListener.class);

	/**
	 * 处理服务下线事件
	 * <p>
	 * 接收并处理服务实例的下线请求，验证请求合法性，从服务注册表中移除实例，
	 * 并在集群模式下处理主从节点关系变更和故障转移逻辑。
	 * <p>
	 * 处理流程：
	 * 1. 验证请求的身份和权限
	 * 2. 获取需要下线的服务实例信息
	 * 3. 判断是否为集群模式下的主节点下线
	 * 4. 如果是主节点下线，执行故障转移和从节点提升流程
	 * 5. 将服务实例从注册表中移除
	 *
	 * @param event 服务下线事件对象，包含下线请求的相关信息
	 * @throws IllegalAccessException 当请求验证失败时抛出异常
	 */
	@Override
	public void onReceive(UnRegistryEvent event) throws IllegalAccessException {
		// 获取事件上下文和请求ID，验证请求合法性
		ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();
		Object reqId = channelHandlerContext.attr(AttributeKey.valueOf("reqId")).get();
		if (reqId == null) {
			// 请求ID不存在，返回错误消息并关闭连接
			channelHandlerContext.writeAndFlush(new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
				NameServerResponseCode.ERROR_USER_OR_PASSWORD.getDesc().getBytes()));
			channelHandlerContext.close();
			throw new IllegalAccessException("error account to connected!");
		}

		// 将请求ID转换为字符串
		String reqIdStr = (String) reqId;

		// 根据请求ID获取需要下线的服务实例
		ServiceInstance needRemoveServiceInstance = CommonCache.getServiceInstanceManager().get(reqIdStr);
		Map<String, Object> attrsMap = needRemoveServiceInstance.getAttrs();

		// 获取节点在集群中的角色和组信息
		String brokerClusterRole = (String) attrsMap.getOrDefault("role", "");
		String brokerClusterGroup = (String) attrsMap.getOrDefault("group", "");

		// 判断是否为集群模式，单机模式不需要特殊处理
		boolean isClusterMode = !StringUtil.isNullOrEmpty(brokerClusterRole) && !StringUtil.isNullOrEmpty(brokerClusterGroup);
		if (isClusterMode) {
			// 如果是集群模式且下线的是主节点，需���处理故障转移
			if ("master".equals(brokerClusterRole)) {
				// 记录主节点失败的日志
				logger.error("master node fail!");

				// 查找相同集群组的所有从节点
				List<ServiceInstance> reloadNodeList = new ArrayList<>();
				for (ServiceInstance serviceInstance : CommonCache.getServiceInstanceManager().getServiceInstanceMap().values()) {
					String matchGroup = (String) serviceInstance.getAttrs().getOrDefault("group", "");
					String matchRole = (String) serviceInstance.getAttrs().getOrDefault("role", "");
					if (matchGroup.equals(brokerClusterGroup) && "slave".equals(matchRole)) {
						reloadNodeList.add(serviceInstance);
					}
				}
				logger.info("get same cluster group slave node:{}", JSON.toJSONString(reloadNodeList));

				// 选择数据版本最新的从节点作为新的主节点
				long maxVersion = 0L;
				ServiceInstance newMasterServiceInstance = null;
				for (ServiceInstance slaveNode : reloadNodeList) {
					long latestVersion = (long) slaveNode.getAttrs().getOrDefault("latestVersion", 0L);
					if (maxVersion <= latestVersion) {
						newMasterServiceInstance = slaveNode;
						maxVersion = latestVersion;
					}
				}

				// 移除原主节点，并将选出的从节点角色更新为主节点
				CommonCache.getServiceInstanceManager().remove(reqIdStr);
				if (newMasterServiceInstance != null) {
					newMasterServiceInstance.getAttrs().put("role", "master");
				}

				// 重新加载集群配置，应用新的主从关系
				CommonCache.getServiceInstanceManager().reload(reloadNodeList);
				logger.info("new cluster node is:{}", JSON.toJSONString(newMasterServiceInstance));
			}
		}

		// 无论是单机模式还是集群模式的非主节点，都直接从注册表中移除
		CommonCache.getServiceInstanceManager().remove(reqIdStr);
	}
}
