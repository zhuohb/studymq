package com.zhb.broker.model;

import lombok.Getter;
import lombok.Setter;

/**
 * master节点broker的地址信息
 */
@Setter
@Getter
public class MasterBrokerModel {

	private String brokerClusterGroup;
	private String brokerIp;
	private String brokerPort;

}
