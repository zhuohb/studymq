package com.zhb.broker.model;

import com.zhb.common.dto.ConsumeMsgAckReqDTO;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ConsumeMsgAckEvent extends Event {

	private ConsumeMsgAckReqDTO consumeMsgAckReqDTO;

}
