package com.zhb.common.enums;

/**
 * @Author idea
 * @Date: Created in 11:11 2024/6/16
 * @Description
 */
public enum ConsumeResultStatus {

    CONSUME_SUCCESS(1),
    CONSUME_LATER(2),
    ;

    int code;

    ConsumeResultStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
