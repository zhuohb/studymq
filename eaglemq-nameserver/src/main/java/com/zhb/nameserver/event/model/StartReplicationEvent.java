package com.zhb.nameserver.event.model;

import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;

/**
 * 从节点首次连接主节点时候发送的事件
 */
@Setter
@Getter
public class StartReplicationEvent extends Event {

	private String user;
	private String password;
	private String slaveIp;
	private String slavePort;

}
