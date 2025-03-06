package com.zhb.nameserver.event.model;

import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created in 16:30 2024/5/18
 * @Description 从节点首次连接主节点时候发送的事件
 */
public class StartReplicationEvent extends Event {

	private String user;
	private String password;
	private String slaveIp;
	private String slavePort;

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSlaveIp() {
		return slaveIp;
	}

	public void setSlaveIp(String slaveIp) {
		this.slaveIp = slaveIp;
	}

	public String getSlavePort() {
		return slavePort;
	}

	public void setSlavePort(String slavePort) {
		this.slavePort = slavePort;
	}
}
