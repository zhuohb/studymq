package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.enums.PullBrokerIpRoleEnum;
import com.zhb.nameserver.event.model.PullBrokerIpEvent;
import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.PullBrokerIpRespDTO;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.enums.RegistryTypeEnum;
import com.zhb.common.event.Listener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Broker地址查询监听器
 * <p>
 * 负责处理客户端发起的Broker地址查询请求，支持按角色和集群组筛选Broker节点
 * 是客户端与Broker建立连接的关键环节，实现服务发现功能
 * <p>
 * 主要功能：
 * 1. 根据客户端请求的角色类型过滤Broker节点（Master/Slave/Single）
 * 2. 根据指定的集群组名称筛选相关Broker
 * 3. 收集符合条件的Broker地址信息并返回给客户端
 * 4. 支持不同部署模式下的地址查询（单机、主从、集群）
 * <p>
 * 使用场景：
 * - 客户端初始化时查询可用的Broker地址
 * - 客户端进行负载均衡或故障转移时重新获取Broker列表
 * - 客户端根据不同业务需求选择特定角色的Broker节点
 */
public class PullBrokerIpListener implements Listener<PullBrokerIpEvent> {

	/**
	 * 处理Broker地址查询事件
	 * <p>
	 * 根据客户端请求的参数（角色、集群组）从注册中心缓存中筛选符合条件的Broker节点，
	 * 并按照角色类型（Master/Slave/Single）分类组织地址信息返回给客户端。
	 * <p>
	 * 处理流程：
	 * 1. 从事件中获取请求的角色类型和集群组名称
	 * 2. 从全局缓存中获取所有已注册的服务实例
	 * 3. 筛选注册类型为BROKER的实例
	 * 4. 按集群组名称进行第一次筛选
	 * 5. 根据请求的角色类型与Broker的角色进行匹配筛选
	 * 6. 将筛选结果按角色分类（Master/Slave/Single）
	 * 7. 构建响应对象并返回给客户端
	 *
	 * @param event 包含客户端查询参数的事件对象
	 * @throws Exception 处理过程中可能出现的异常
	 */
	@Override
	public void onReceive(PullBrokerIpEvent event) throws Exception {
		// 获取客户端请求的角色类型
		String pullRole = event.getRole();

		// 创建响应对象，用于返回查询结果
		PullBrokerIpRespDTO pullBrokerIpRespDTO = new PullBrokerIpRespDTO();

		// 初始化不同类型的地址列表容器
		List<String> addressList = new ArrayList<>();      // 单机模式Broker地址
		List<String> masterAddressList = new ArrayList<>(); // 主节点地址列表
		List<String> slaveAddressList = new ArrayList<>();  // 从节点地址列表

		// 从缓存中获取所有已注册的服务��例
		Map<String, ServiceInstance> serviceInstanceMap = CommonCache.getServiceInstanceManager().getServiceInstanceMap();

		// 遍历所有服务实例，筛选符合条件的Broker
		for (String reqId : serviceInstanceMap.keySet()) {
			ServiceInstance serviceInstance = serviceInstanceMap.get(reqId);

			// 仅处理注册类型为BROKER的实例
			if (RegistryTypeEnum.BROKER.getCode().equals(serviceInstance.getRegistryType())) {
				// 获取Broker的附加属性
				Map<String, Object> brokerAttrs = serviceInstance.getAttrs();
				String group = (String) brokerAttrs.getOrDefault("group", "");

				// 先匹配集群组，再根据角色进行筛选
				if (group.equals(event.getBrokerClusterGroup())) {
					String role = (String) brokerAttrs.get("role");

					// 根据角色类型将Broker地址添加到相应的列表中
					if (PullBrokerIpRoleEnum.MASTER.getCode().equals(pullRole)
						&& PullBrokerIpRoleEnum.MASTER.getCode().equals(role)) {
						// 客户端请求Master角色且Broker是Master角色
						masterAddressList.add(serviceInstance.getIp() + ":" + serviceInstance.getPort());
					} else if (PullBrokerIpRoleEnum.SLAVE.getCode().equals(pullRole)
						&& PullBrokerIpRoleEnum.SLAVE.getCode().equals(role)) {
						// 客户端请求Slave角色且Broker是Slave角色
						slaveAddressList.add(serviceInstance.getIp() + ":" + serviceInstance.getPort());
					} else if (PullBrokerIpRoleEnum.SINGLE.getCode().equals(pullRole)
						&& PullBrokerIpRoleEnum.SINGLE.getCode().equals(role)) {
						// 客户端请求Single角色且Broker是Single角色（单机模式）
						addressList.add(serviceInstance.getIp() + ":" + serviceInstance.getPort());
					}
				}
			}
		}

		// 设置响应对象的属性
		pullBrokerIpRespDTO.setMsgId(event.getMsgId());
		pullBrokerIpRespDTO.setMasterAddressList(masterAddressList);
		pullBrokerIpRespDTO.setSlaveAddressList(slaveAddressList);
		// 去重后设置单机模式地址列表
		pullBrokerIpRespDTO.setAddressList(addressList.stream().distinct().collect(Collectors.toList()));

		// 将响应发送回客户端
		event.getChannelHandlerContext().writeAndFlush(new TcpMsg(
			NameServerResponseCode.PULL_BROKER_ADDRESS_SUCCESS.getCode(),
			JSON.toJSONBytes(pullBrokerIpRespDTO)));
	}
}
