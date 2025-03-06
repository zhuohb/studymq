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
 * @Author idea
 * @Date: Created in 16:48 2024/6/11
 * @Description
 */
public class PullBrokerIpListener implements Listener<PullBrokerIpEvent> {

	@Override
	public void onReceive(PullBrokerIpEvent event) throws Exception {
		String pullRole = event.getRole();
		PullBrokerIpRespDTO pullBrokerIpRespDTO = new PullBrokerIpRespDTO();
		List<String> addressList = new ArrayList<>();
		List<String> masterAddressList = new ArrayList<>();
		List<String> slaveAddressList = new ArrayList<>();
		Map<String, ServiceInstance> serviceInstanceMap = CommonCache.getServiceInstanceManager().getServiceInstanceMap();
		for (String reqId : serviceInstanceMap.keySet()) {
			ServiceInstance serviceInstance = serviceInstanceMap.get(reqId);
			if (RegistryTypeEnum.BROKER.getCode().equals(serviceInstance.getRegistryType())) {
				Map<String, Object> brokerAttrs = serviceInstance.getAttrs();
				String group = (String) brokerAttrs.getOrDefault("group", "");
				//先命中集群组，再根据角色进行判断
				if (group.equals(event.getBrokerClusterGroup())) {
					String role = (String) brokerAttrs.get("role");
					if (PullBrokerIpRoleEnum.MASTER.getCode().equals(pullRole)
						&& PullBrokerIpRoleEnum.MASTER.getCode().equals(role)) {
						masterAddressList.add(serviceInstance.getIp() + ":" + serviceInstance.getPort());
					} else if (PullBrokerIpRoleEnum.SLAVE.getCode().equals(pullRole)
						&& PullBrokerIpRoleEnum.SLAVE.getCode().equals(role)) {
						slaveAddressList.add(serviceInstance.getIp() + ":" + serviceInstance.getPort());
					} else if (PullBrokerIpRoleEnum.SINGLE.getCode().equals(pullRole)
						&& PullBrokerIpRoleEnum.SINGLE.getCode().equals(role)) {
						addressList.add(serviceInstance.getIp() + ":" + serviceInstance.getPort());
					}
				}
			}
		}
		pullBrokerIpRespDTO.setMsgId(event.getMsgId());
		pullBrokerIpRespDTO.setMasterAddressList(masterAddressList);
		pullBrokerIpRespDTO.setSlaveAddressList(slaveAddressList);
		//防止ip重复
		pullBrokerIpRespDTO.setAddressList(addressList.stream().distinct().collect(Collectors.toList()));
		event.getChannelHandlerContext().writeAndFlush(new TcpMsg(NameServerResponseCode.PULL_BROKER_ADDRESS_SUCCESS.getCode(),
			JSON.toJSONBytes(pullBrokerIpRespDTO)));
	}
}
