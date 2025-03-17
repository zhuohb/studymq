package com.zhb.common.enums;

import lombok.Getter;

/**
 * 请求nameserver的响应码
 */
@Getter
public enum NameServerResponseCode {

	ERROR_USER_OR_PASSWORD(1001, "账号验证异常"),
	UN_REGISTRY_SERVICE(1002, "服务正常下线"),
	REGISTRY_SUCCESS(1003, "注册成功"),
	HEART_BEAT_SUCCESS(1004, "心跳ACK"),
	PULL_BROKER_ADDRESS_SUCCESS(1005, "拉broker地址成功"),
	;


	private final int code;
	private final String desc;

	NameServerResponseCode(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

}
