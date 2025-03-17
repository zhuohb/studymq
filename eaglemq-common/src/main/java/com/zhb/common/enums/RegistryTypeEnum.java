package com.zhb.common.enums;

import lombok.Getter;

/**
 * 注册类型
 */
@Getter
public enum RegistryTypeEnum {

	PRODUCER("producer"),
	CONSUMER("consumer"),
	BROKER("broker");
	private final String code;

	RegistryTypeEnum(String code) {
		this.code = code;
	}

}
