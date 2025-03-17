package com.zhb.common.dto;

import com.zhb.common.enums.AckStatus;
import lombok.Getter;
import lombok.Setter;

/**
 * 消息重试确认返回DTO
 */
@Setter
@Getter
public class ConsumeMsgRetryRespDTO extends BaseBrokerRemoteDTO{

    /**
     * ack是否成功
     * @see AckStatus
     */
    private int ackStatus;

}
