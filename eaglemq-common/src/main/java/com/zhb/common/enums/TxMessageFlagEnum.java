package com.zhb.common.enums;

/**
 * @Author idea
 * @Date: Created at 2024/8/17
 * @Description 事务消息类型枚举
 */
public enum TxMessageFlagEnum {

    HALF_MSG(0,"半提交消息"),
    REMAIN_HALF_ACK(1,"剩余半条ack消息");

    int code;
    String desc;

    TxMessageFlagEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
