package com.zhb.common.enums;

import lombok.Getter;

/**
 * 事务消息类型枚举
 */
@Getter
public enum TxMessageFlagEnum {

	HALF_MSG(0, "半提交消息"),
	REMAIN_HALF_ACK(1, "剩余半条ack消息");

	private final int code;
	private final String desc;

	TxMessageFlagEnum(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

}
