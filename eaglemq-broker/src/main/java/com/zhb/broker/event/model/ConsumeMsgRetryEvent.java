package com.zhb.broker.event.model;

import com.zhb.common.dto.ConsumeMsgRetryReqDTO;
import com.zhb.common.dto.ConsumeMsgRetryReqDetailDTO;
import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created at 2024/7/21
 * @Description
 */
public class ConsumeMsgRetryEvent extends Event {

    private ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO;

    public ConsumeMsgRetryReqDTO getConsumeMsgRetryReqDTO() {
        return consumeMsgRetryReqDTO;
    }

    public void setConsumeMsgRetryReqDTO(ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO) {
        this.consumeMsgRetryReqDTO = consumeMsgRetryReqDTO;
    }
}
