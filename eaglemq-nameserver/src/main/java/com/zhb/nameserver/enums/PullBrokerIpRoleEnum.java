package com.zhb.nameserver.enums;

/**
 * @Author idea
 * @Date: Created in 16:49 2024/6/11
 * @Description
 */
public enum PullBrokerIpRoleEnum {

	MASTER("master"),
	SLAVE("slave"),
	SINGLE("single"),
	;
	String code;

	PullBrokerIpRoleEnum(String code) {
		this.code = code;
	}

	public String getCode() {
		return code;
	}
}
