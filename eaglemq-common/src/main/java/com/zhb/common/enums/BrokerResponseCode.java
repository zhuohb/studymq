package com.zhb.common.enums;


import lombok.Getter;

@Getter
public enum BrokerResponseCode {

    SEND_MSG_RESP(2001,"推送消息给broker，响应code"),
    CONSUME_MSG_RESP(2002,"消费broker消息返回数据，响应code"),
    BROKER_UPDATE_CONSUME_OFFSET_RESP(2003,"broker更新消费offset，响应code"),
    CREATED_TOPIC_SUCCESS(2004,"创建topic成功，响应code"),
    START_SYNC_SUCCESS(2005,"主从开始同步数据，响应code"),
    SLAVE_SYNC_RESP(2006,"从节点同步数据，响应code"),
    CONSUME_MSG_RETRY_RESP(2007,"消息重试响应code"),
    HALF_MSG_SEND_SUCCESS(2008,"half消息发送成功"),
    REMAIN_ACK_MSG_SEND_SUCCESS(2009,"剩余事务消息ack成功"),
    TX_CALLBACK_MSG(2010,"事务消息回调信号"),
    ;

    private final int code;
    private final String desc;

    BrokerResponseCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

}
