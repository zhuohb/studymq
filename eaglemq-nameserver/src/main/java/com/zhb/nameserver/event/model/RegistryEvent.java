package com.zhb.nameserver.event.model;

import com.zhb.common.enums.RegistryTypeEnum;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * 注册事件（首次链接nameserver使用）
 */
@Setter
@Getter
public class RegistryEvent extends Event {

	/**
	 * 节点的注册类型，方便统计数据使用
	 *
	 * @see RegistryTypeEnum
	 */
	private String registryType;
	private String user;
	private String password;
	private String ip;
	private Integer port;
	private Map<String, Object> attrs = new HashMap<>();

}
