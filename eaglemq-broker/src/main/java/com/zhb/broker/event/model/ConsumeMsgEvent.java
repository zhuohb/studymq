package com.zhb.broker.event.model;

import com.zhb.common.dto.ConsumeMsgReqDTO;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;

/**
 * 消费mq消息事件
 */
@Setter
@Getter
public class ConsumeMsgEvent extends Event {

	private ConsumeMsgReqDTO consumeMsgReqDTO;

}
