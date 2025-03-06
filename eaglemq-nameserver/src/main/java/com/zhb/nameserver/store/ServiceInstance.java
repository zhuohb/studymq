package com.zhb.nameserver.store;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 17:36 2024/5/4
 * @Description 服务实例
 */
public class ServiceInstance {

	/**
	 * 注册类型
	 *
	 * @see com.zhb.common.enums.RegistryTypeEnum
	 */
	private String registryType;
	private String ip;
	private Integer port;
	private Long firstRegistryTime;
	private Long lastHeartBeatTime;
	private Map<String, Object> attrs = new HashMap<>();

	public String getRegistryType() {
		return registryType;
	}

	public void setRegistryType(String registryType) {
		this.registryType = registryType;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Long getFirstRegistryTime() {
		return firstRegistryTime;
	}

	public void setFirstRegistryTime(Long firstRegistryTime) {
		this.firstRegistryTime = firstRegistryTime;
	}

	public Long getLastHeartBeatTime() {
		return lastHeartBeatTime;
	}

	public void setLastHeartBeatTime(Long lastHeartBeatTime) {
		this.lastHeartBeatTime = lastHeartBeatTime;
	}

	public Map<String, Object> getAttrs() {
		return attrs;
	}

	public void setAttrs(Map<String, Object> attrs) {
		this.attrs = attrs;
	}
}
