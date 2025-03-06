package com.zhb.nameserver.store;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author idea
 * @Date: Created in 17:37 2024/5/4
 * @Description
 */
public class ServiceInstanceManager {

	private Map<String, ServiceInstance> serviceInstanceMap = new ConcurrentHashMap<>();

	public void putIfExist(ServiceInstance serviceInstance) {
		ServiceInstance currentInstance = this.get(serviceInstance.getIp(), serviceInstance.getPort());
		if (currentInstance != null && currentInstance.getFirstRegistryTime() != null) {
			currentInstance.setLastHeartBeatTime(serviceInstance.getLastHeartBeatTime());
			serviceInstanceMap.put(serviceInstance.getIp() + ":" + serviceInstance.getPort(), currentInstance);
		} else {
			throw new RuntimeException("之前心跳缓存已经剔除，请重新注册");
		}
	}

	//todo 加锁
	public void reload(List<ServiceInstance> serviceInstanceList) {
		serviceInstanceMap.clear();
		for (ServiceInstance serviceInstance : serviceInstanceList) {
			this.put(serviceInstance);
		}
	}

	public void put(ServiceInstance serviceInstance) {
		serviceInstanceMap.put(serviceInstance.getIp() + ":" + serviceInstance.getPort(), serviceInstance);
	}

	public ServiceInstance get(String brokerIp, Integer brokerPort) {
		return serviceInstanceMap.get(brokerIp + ":" + brokerPort);
	}

	public ServiceInstance get(String reqId) {
		return serviceInstanceMap.get(reqId);
	}

	public ServiceInstance remove(String key) {
		return serviceInstanceMap.remove(key);
	}

	public Map<String, ServiceInstance> getServiceInstanceMap() {
		return serviceInstanceMap;
	}
}
