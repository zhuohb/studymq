package com.zhb.nameserver.event.model;

import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created in 16:56 2024/5/18
 * @Description 复制消息
 */
public class ReplicationMsgEvent extends Event {

	private Integer type;

	private ServiceInstance serviceInstance;

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public ServiceInstance getServiceInstance() {
		return serviceInstance;
	}

	public void setServiceInstance(ServiceInstance serviceInstance) {
		this.serviceInstance = serviceInstance;
	}
}
