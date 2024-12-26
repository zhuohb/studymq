package com.zhb.config;

import com.zhb.cache.CommonCache;
import com.zhb.constants.BrokerConstants;

/**
 * 加载mq的配置属性
 */
public class GlobalPropertiesLoader {


	/**
	 * 加载mq的配置属性
	 */
	public void loadProperties() {
		GlobalProperties globalProperties = new GlobalProperties();
		//开发的时候先写死路径吧
//		String property = System.getenv(BrokerConstants.MQ_HOME);
		String property = "D:\\code\\zhuohb\\backend\\studymq";
		if (property == null || property.isEmpty()) {
			throw new IllegalArgumentException("MQ_HOME is null");
		}
		globalProperties.setMqHome(property);
		CommonCache.setGlobalProperties(globalProperties);
	}
}
