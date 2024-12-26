package com.zhb.config;

import com.alibaba.fastjson.JSON;
import com.zhb.cache.CommonCache;
import com.zhb.model.MqTopicModel;
import com.zhb.utils.FileContentReaderUtil;

import java.util.List;

public class TopicInfoLoader {


	public void loadProperties() {
		GlobalProperties globalProperties = CommonCache.getGlobalProperties();
		String basePath = globalProperties.getMqHome();
		if (basePath == null || basePath.isEmpty()) {
			throw new IllegalArgumentException("MQ_HOME is null");
		}
		String topicJsonFilePath = basePath + "\\broker\\config\\json\\mq-topic.json";
		String fileContent = FileContentReaderUtil.readFromFile(topicJsonFilePath);
		List<MqTopicModel> modelList = JSON.parseArray(fileContent, MqTopicModel.class);
		CommonCache.setTopicModelList(modelList);
	}
}
