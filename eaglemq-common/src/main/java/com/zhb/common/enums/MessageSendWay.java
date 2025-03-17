package com.zhb.common.enums;

import lombok.Getter;


@Getter
public enum MessageSendWay {

    SYNC(1),
    ASYNC(2),
    ;

    private final int code;

    MessageSendWay(int code) {
        this.code = code;
    }

}
