package com.zhb.nameserver.event.model;

import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class NodeReplicationMsgEvent extends Event {

	private Integer type;
	private ServiceInstance serviceInstance;

}
