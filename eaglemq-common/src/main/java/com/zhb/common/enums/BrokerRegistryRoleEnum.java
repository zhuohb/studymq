package com.zhb.common.enums;


import lombok.Getter;

@Getter
public enum BrokerRegistryRoleEnum {

	MASTER("master"),
	SLAVE("slave"),
	SINGLE("single"),
	;

	private String code;

	BrokerRegistryRoleEnum(String code) {
		this.code = code;
	}

	public static BrokerRegistryRoleEnum of(String code) {
		for (BrokerRegistryRoleEnum brokerRegistryEnum : BrokerRegistryRoleEnum.values()) {
			if (brokerRegistryEnum.getCode().equals(code)) {
				return brokerRegistryEnum;
			}
		}
		return null;
	}
}
