package com.zhb.nameserver.event.model;

import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created in 16:48 2024/6/11
 * @Description
 */
public class PullBrokerIpEvent extends Event {

	private String role;

	private String brokerClusterGroup;

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getBrokerClusterGroup() {
		return brokerClusterGroup;
	}

	public void setBrokerClusterGroup(String brokerClusterGroup) {
		this.brokerClusterGroup = brokerClusterGroup;
	}
}
