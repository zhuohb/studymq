package com.zhb.common.dto;


import com.zhb.common.enums.AckStatus;

/**
 * @author idea
 * @create 2024/6/26 08:46
 * @description
 */
public class ConsumeMsgAckRespDTO extends BaseBrokerRemoteDTO{

    /**
     * ack是否成功
     * @see AckStatus
     */
    private int ackStatus;

    public int getAckStatus() {
        return ackStatus;
    }

    public void setAckStatus(int ackStatus) {
        this.ackStatus = ackStatus;
    }
}
