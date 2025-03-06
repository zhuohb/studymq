package com.zhb.common.enums;

/**
 * @Author idea
 * @Date: Created in 20:18 2024/6/15
 * @Description
 */
public enum MessageSendWay {

    SYNC(1),
    ASYNC(2),
    ;

    int code;

    MessageSendWay(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
