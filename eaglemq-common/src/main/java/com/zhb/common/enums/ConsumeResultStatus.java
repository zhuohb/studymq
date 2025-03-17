package com.zhb.common.enums;

import lombok.Getter;


@Getter
public enum ConsumeResultStatus {

    CONSUME_SUCCESS(1),
    CONSUME_LATER(2),
    ;

    private final int code;

    ConsumeResultStatus(int code) {
        this.code = code;
    }

}
