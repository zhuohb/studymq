package com.zhb.nameserver.event.model;

import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created in 10:16 2024/6/1
 * @Description
 */
public class NodeReplicationMsgEvent extends Event {

	private Integer type;
	private ServiceInstance serviceInstance;

	public ServiceInstance getServiceInstance() {
		return serviceInstance;
	}

	public void setServiceInstance(ServiceInstance serviceInstance) {
		this.serviceInstance = serviceInstance;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}
}
