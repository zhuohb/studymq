package com.zhb.broker.event.model;

import com.zhb.common.dto.ConsumeMsgRetryReqDTO;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class ConsumeMsgRetryEvent extends Event {

	private ConsumeMsgRetryReqDTO consumeMsgRetryReqDTO;

}
