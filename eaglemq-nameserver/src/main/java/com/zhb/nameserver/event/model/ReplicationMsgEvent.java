package com.zhb.nameserver.event.model;

import com.zhb.common.event.model.Event;
import com.zhb.nameserver.store.ServiceInstance;
import lombok.Getter;
import lombok.Setter;

/**
 * 复制消息
 */
@Setter
@Getter
public class ReplicationMsgEvent extends Event {

	private Integer type;

	private ServiceInstance serviceInstance;

}
