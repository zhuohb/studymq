package com.zhb.nameserver.common;

import com.zhb.nameserver.enums.MasterSlaveReplicationTypeEnum;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class MasterSlaveReplicationProperties {

	private String master;

	private String role;

	/**
	 * @see MasterSlaveReplicationTypeEnum
	 */
	private String type;

	private Integer port;

}
