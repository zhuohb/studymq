package com.zhb.config;

import lombok.Data;

@Data
public class GlobalProperties {

	/**
	 * 读取环境变量中配置的mq存储绝对路径地址
	 */
	private String eagleMqHome;
}
