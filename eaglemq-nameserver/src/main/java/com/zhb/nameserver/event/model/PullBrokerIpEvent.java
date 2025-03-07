package com.zhb.nameserver.event.model;

import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class PullBrokerIpEvent extends Event {

	private String role;

	private String brokerClusterGroup;

}
