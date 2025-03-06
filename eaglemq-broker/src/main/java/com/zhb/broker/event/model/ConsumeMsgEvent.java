package com.zhb.broker.event.model;

import com.zhb.common.dto.ConsumeMsgReqDTO;
import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created in 22:18 2024/6/19
 * @Description 消费mq消息事件
 */
public class ConsumeMsgEvent extends Event {

    private ConsumeMsgReqDTO consumeMsgReqDTO;

    public ConsumeMsgReqDTO getConsumeMsgReqDTO() {
        return consumeMsgReqDTO;
    }

    public void setConsumeMsgReqDTO(ConsumeMsgReqDTO consumeMsgReqDTO) {
        this.consumeMsgReqDTO = consumeMsgReqDTO;
    }
}
