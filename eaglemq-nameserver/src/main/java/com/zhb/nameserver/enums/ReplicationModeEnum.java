package com.zhb.nameserver.enums;


import lombok.Getter;

@Getter
public enum ReplicationModeEnum {

	MASTER_SLAVE("master_slave", "主从复制模式"),
	TRACE("trace", "链路复制模式");
	final String code;
	final String desc;

	ReplicationModeEnum(String code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	public static ReplicationModeEnum of(String code) {
		for (ReplicationModeEnum value : values()) {
			if (value.getCode().equals(code)) {
				return value;
			}
		}
		return null;
	}

}
