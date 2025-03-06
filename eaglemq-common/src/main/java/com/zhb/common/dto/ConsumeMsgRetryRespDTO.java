package com.zhb.common.dto;

import com.zhb.common.enums.AckStatus;

/**
 * @Author idea
 * @Date: Created at 2024/7/21
 * @Description 消息重试确认返回DTO
 */
public class ConsumeMsgRetryRespDTO extends BaseBrokerRemoteDTO{

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
