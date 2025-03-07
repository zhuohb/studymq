package com.zhb.broker.event.model;

import com.zhb.common.dto.MessageDTO;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class PushMsgEvent extends Event {

	private MessageDTO messageDTO;

}
