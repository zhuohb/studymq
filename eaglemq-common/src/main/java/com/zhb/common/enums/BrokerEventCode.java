package com.zhb.common.enums;

import lombok.Getter;

/**
 * broker服务端事件处理code
 */
@Getter
public enum BrokerEventCode {

	PUSH_MSG(1001, "推送消息"),
	CONSUME_MSG(1002, "消费消息"),
	CONSUME_SUCCESS_MSG(1003, "消费成功"),
	CREATE_TOPIC(1004, "创建topic"),
	START_SYNC_MSG(1005, "从节点开启同步"),
	CONSUME_LATER_MSG(1006, "消息重试"),
	;

	private final int code;
	private final String desc;

	BrokerEventCode(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

}
