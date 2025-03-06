package com.zhb.nameserver.enums;

/**
 * @Author idea
 * @Date: Created in 15:30 2024/6/1
 * @Description 复制数据类型枚举
 */
public enum ReplicationMsgTypeEnum {

	REGISTRY(1, "节点复制"),
	HEART_BEAT(2, "心跳");

	ReplicationMsgTypeEnum(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	int code;
	String desc;

	public int getCode() {
		return code;
	}

	public String getDesc() {
		return desc;
	}
}
