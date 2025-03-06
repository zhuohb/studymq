package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.UnRegistryEvent;
import com.zhb.nameserver.store.ServiceInstance;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.internal.StringUtil;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 14:44 2024/5/4
 * @Description
 */
public class UnRegistryListener implements Listener<UnRegistryEvent> {

	private final Logger logger = LoggerFactory.getLogger(UnRegistryListener.class);

	@Override
	public void onReceive(UnRegistryEvent event) throws IllegalAccessException {
		ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();
		Object reqId = channelHandlerContext.attr(AttributeKey.valueOf("reqId")).get();
		if (reqId == null) {
			channelHandlerContext.writeAndFlush(new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
				NameServerResponseCode.ERROR_USER_OR_PASSWORD.getDesc().getBytes()));
			channelHandlerContext.close();
			throw new IllegalAccessException("error account to connected!");
		}
		String reqIdStr = (String) reqId;
		//在下线监听器里识别master节点端开的场景
		ServiceInstance needRemoveServiceInstance = CommonCache.getServiceInstanceManager().get(reqIdStr);
		Map<String, Object> attrsMap = needRemoveServiceInstance.getAttrs();
		String brokerClusterRole = (String) attrsMap.getOrDefault("role", "");
		String brokerClusterGroup = (String) attrsMap.getOrDefault("group", "");
		//单机架构不需要处理
		boolean isClusterMode = !StringUtil.isNullOrEmpty(brokerClusterRole) && !StringUtil.isNullOrEmpty(brokerClusterGroup);
		if (isClusterMode) {
			//集群模式+主节点
			if ("master".equals(brokerClusterRole)) {
				//todo 主从切换逻辑处理
				logger.error("master node fail!");
				List<ServiceInstance> reloadNodeList = new ArrayList<>();
				for (ServiceInstance serviceInstance : CommonCache.getServiceInstanceManager().getServiceInstanceMap().values()) {
					String matchGroup = (String) serviceInstance.getAttrs().getOrDefault("group", "");
					String matchRole = (String) serviceInstance.getAttrs().getOrDefault("role", "");
					if (matchGroup.equals(brokerClusterGroup) && "slave".equals(matchRole)) {
						reloadNodeList.add(serviceInstance);
					}
				}
				logger.info("get same cluster group slave node:{}", JSON.toJSONString(reloadNodeList));
				//将之前的master节点移除，然后选择从节点中版本号最新的一方作为新的主节点
				long maxVersion = 0L;
				ServiceInstance newMasterServiceInstance = null;
				for (ServiceInstance slaveNode : reloadNodeList) {
					long latestVersion = (long) slaveNode.getAttrs().getOrDefault("latestVersion", 0L);
					if (maxVersion <= latestVersion) {
						newMasterServiceInstance = slaveNode;
						maxVersion = latestVersion;
					}
				}
				CommonCache.getServiceInstanceManager().remove(reqIdStr);
				if (newMasterServiceInstance != null) {
					newMasterServiceInstance.getAttrs().put("role", "master");
				}
				//重新设置主从关系
				CommonCache.getServiceInstanceManager().reload(reloadNodeList);
				logger.info("new cluster node is:{}", JSON.toJSONString(newMasterServiceInstance));
			}
		}
		CommonCache.getServiceInstanceManager().remove(reqIdStr);
	}
}
