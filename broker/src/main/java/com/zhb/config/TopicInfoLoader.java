package com.zhb.config;

import com.zhb.cache.CommonCache;

public class TopicInfoLoader {

	private TopicInfo topicInfo;

	public void loadProperties() {
		GlobalProperties globalProperties = CommonCache.getGlobalProperties();
		String basePath = globalProperties.getMqHome();
		if (basePath == null || basePath.isEmpty()) {
			throw new IllegalArgumentException("MQ_HOME is null");
		}
		String topicJsonFilePath = basePath + "\\broker\\config\\json\\mq-topic.json";
		topicInfo = new TopicInfo();
	}
}
