package com.zhb.nameserver.store;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * 服务实例
 */
@Setter
@Getter
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

}
