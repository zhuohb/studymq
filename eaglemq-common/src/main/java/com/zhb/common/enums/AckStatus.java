package com.zhb.common.enums;

/**
 * @author idea
 * @create 2024/6/26 08:47
 * @description
 */
public enum AckStatus {

    SUCCESS(1),
    FAIL(0),
    ;

    AckStatus(int code) {
        this.code = code;
    }

    int code;

    public int getCode() {
        return code;
    }
}
