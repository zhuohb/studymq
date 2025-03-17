package com.zhb.common.dto;


import com.zhb.common.enums.AckStatus;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class ConsumeMsgAckRespDTO extends BaseBrokerRemoteDTO{

    /**
     * ack是否成功
     * @see AckStatus
     */
    private int ackStatus;

}
