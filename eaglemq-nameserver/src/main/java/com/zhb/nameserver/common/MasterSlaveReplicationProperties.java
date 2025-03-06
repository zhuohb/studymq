package com.zhb.nameserver.common;

import com.zhb.nameserver.enums.MasterSlaveReplicationTypeEnum;

/**
 * @Author idea
 * @Date: Created in 09:06 2024/5/15
 * @Description
 */
public class MasterSlaveReplicationProperties {

	private String master;

	private String role;

	/**
	 * @see MasterSlaveReplicationTypeEnum
	 */
	private String type;

	private Integer port;

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
