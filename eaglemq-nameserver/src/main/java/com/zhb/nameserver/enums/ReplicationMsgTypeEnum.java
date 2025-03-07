package com.zhb.nameserver.enums;

import lombok.Getter;

/**
 * 复制数据类型枚举
 */
@Getter
public enum ReplicationMsgTypeEnum {

	REGISTRY(1, "节点复制"),
	HEART_BEAT(2, "心跳");

	ReplicationMsgTypeEnum(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	final int code;
	final String desc;

}
