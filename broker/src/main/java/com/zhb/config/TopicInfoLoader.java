package com.zhb.config;

import com.alibaba.fastjson.JSON;
import com.zhb.cache.CommonCache;
import com.zhb.model.TopicModel;
import com.zhb.utils.FileContentReaderUtil;

import java.util.List;
import java.util.stream.Collectors;

public class TopicInfoLoader {


	public void loadProperties() {
		GlobalProperties globalProperties = CommonCache.getGlobalProperties();
		String basePath = globalProperties.getMqHome();
		if (basePath == null || basePath.isEmpty()) {
			throw new IllegalArgumentException("MQ_HOME is null");
		}
		String topicJsonFilePath = basePath + "\\broker\\config\\json\\mq-topic.json";
		String fileContent = FileContentReaderUtil.readFromFile(topicJsonFilePath);
		List<TopicModel> modelList = JSON.parseArray(fileContent, TopicModel.class);
		CommonCache.setTopicModelMap(modelList.stream().collect(Collectors.toMap(TopicModel::getTopic, item -> item)));

	}
}
