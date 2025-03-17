package com.zhb.common.enums;


import lombok.Getter;

@Getter
public enum AckStatus {

    SUCCESS(1),
    FAIL(0),
    ;

    AckStatus(int code) {
        this.code = code;
    }

	private final int code;

}
