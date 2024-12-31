package com.zhb.config;

import com.zhb.cache.CommonCache;
import com.zhb.constants.BrokerConstants;
import io.netty.util.internal.StringUtil;

/**
 * 属性初始化
 * EAGLE_MQ_HOME需要配置环境变量,开发的时候可以写死
 */
public class GlobalPropertiesLoader {

	public void loadProperties() {
		GlobalProperties globalProperties = new GlobalProperties();
		String eagleMqHome = System.getenv(BrokerConstants.EAGLE_MQ_HOME);
		if (StringUtil.isNullOrEmpty(eagleMqHome)) {
			throw new IllegalArgumentException("EAGLE_MQ_HOME is null");
		}
		globalProperties.setEagleMqHome(eagleMqHome);
		CommonCache.setGlobalProperties(globalProperties);
	}
}
