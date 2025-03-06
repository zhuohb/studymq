package com.zhb.broker.model;

import com.zhb.common.dto.ConsumeMsgAckReqDTO;
import com.zhb.common.event.model.Event;

/**
 * @author idea
 * @create 2024/6/26 08:39
 * @description
 */
public class ConsumeMsgAckEvent extends Event {

    private ConsumeMsgAckReqDTO consumeMsgAckReqDTO;

    public ConsumeMsgAckReqDTO getConsumeMsgAckReqDTO() {
        return consumeMsgAckReqDTO;
    }

    public void setConsumeMsgAckReqDTO(ConsumeMsgAckReqDTO consumeMsgAckReqDTO) {
        this.consumeMsgAckReqDTO = consumeMsgAckReqDTO;
    }
}
