package com.zhb.broker.event.model;

import com.zhb.common.dto.CreateTopicReqDTO;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;

/**
 * 创建topic事件
 */
@Setter
@Getter
public class CreateTopicEvent extends Event {

	private CreateTopicReqDTO createTopicReqDTO;

}
