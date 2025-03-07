package com.zhb.nameserver.event.model;

import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class NodeReplicationAckMsgEvent extends Event {

	private Integer type;

	private String nodeIp;

	private Integer nodePort;

}
