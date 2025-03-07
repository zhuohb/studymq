package com.zhb.nameserver.common;

import lombok.Getter;
import lombok.Setter;

/**
 * 链路化方式同步配置
 */
@Setter
@Getter
public class TraceReplicationProperties {

	private String nextNode;

	private Integer port;

}
