package com.zhb.nameserver.enums;

/**
 * @Author idea
 * @Date: Created in 16:11 2024/5/18
 * @Description
 */
public enum ReplicationModeEnum {

	MASTER_SLAVE("master_slave", "主从复制模式"),
	TRACE("trace", "链路复制模式");
	String code;
	String desc;

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

	public String getCode() {
		return code;
	}

	public String getDesc() {
		return desc;
	}
}
