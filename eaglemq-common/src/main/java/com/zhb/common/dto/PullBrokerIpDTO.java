package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PullBrokerIpDTO extends BaseNameServerRemoteDTO {

	private String role;

	private String brokerClusterGroup;

}
