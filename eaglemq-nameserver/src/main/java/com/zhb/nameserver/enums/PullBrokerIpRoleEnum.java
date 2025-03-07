package com.zhb.nameserver.enums;


import lombok.Getter;

@Getter
public enum PullBrokerIpRoleEnum {

	MASTER("master"),
	SLAVE("slave"),
	SINGLE("single"),
	;
	final String code;

	PullBrokerIpRoleEnum(String code) {
		this.code = code;
	}

}
