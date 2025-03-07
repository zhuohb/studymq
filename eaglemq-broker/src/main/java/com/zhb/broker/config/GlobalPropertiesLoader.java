package com.zhb.broker.config;

import com.zhb.broker.cache.CommonCache;
import com.zhb.common.constants.BrokerConstants;
import io.netty.util.internal.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

/**
 * 全局属性加载器
 * 负责从配置文件中加载EagleMQ代理的全局配置属性
 */
public class GlobalPropertiesLoader {
	/**
	 * 加载全局配置属性
	 * 从环境变量和配置文件中读取必要的配置信息，并存储到CommonCache中
	 */
	public void loadProperties() {
		GlobalProperties globalProperties = new GlobalProperties();
		// 从环境变量获取EagleMQ的安装路径
		String eagleMqHome = System.getenv(BrokerConstants.EAGLE_MQ_HOME);
		if (StringUtil.isNullOrEmpty(eagleMqHome)) {
			throw new IllegalArgumentException("EAGLE_MQ_HOME is null");
		}
		globalProperties.setEagleMqHome(eagleMqHome);
		// 加载属性文件
		Properties properties = new Properties();
		try {
			// 从指定路径加载配置文件
			properties.load(Files.newInputStream(new File(eagleMqHome + BrokerConstants.BROKER_PROPERTIES_PATH).toPath()));
			// 读取NameServer相关配置
			globalProperties.setNameserverIp(properties.getProperty("nameserver.ip"));
			globalProperties.setNameserverPort(Integer.valueOf(properties.getProperty("nameserver.port")));
			globalProperties.setNameserverUser(properties.getProperty("nameserver.user"));
			globalProperties.setNameserverPassword(properties.getProperty("nameserver.password"));
			// 读取Broker相关配置
			globalProperties.setBrokerPort(Integer.valueOf(properties.getProperty("broker.port")));
			// 读取负载均衡策略
			globalProperties.setReBalanceStrategy(properties.getProperty("rebalance.strategy"));
			// 读取集群相关配置
			globalProperties.setBrokerClusterGroup(properties.getProperty("broker.cluster.group"));
			globalProperties.setBrokerClusterMode(properties.getProperty("broker.cluster.mode"));
			globalProperties.setBrokerClusterRole(properties.getProperty("broker.cluster.role"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// 将加载的全局配置存入公共缓存
		CommonCache.setGlobalProperties(globalProperties);
	}
}
