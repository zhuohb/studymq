package com.zhb.broker.config;


import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class GlobalProperties {

	//nameserver的属性
	private String nameserverIp;
	private Integer nameserverPort;
	private String nameserverUser;
	private String nameserverPassword;
	private Integer brokerPort;
	//重平衡策略
	private String reBalanceStrategy;

	//集群配置相关属性
	private String brokerClusterMode;
	private String brokerClusterRole;
	private String brokerClusterGroup;

	/**
	 * 读取环境变量中配置的mq存储绝对路径地址
	 */
	private String eagleMqHome;

}
