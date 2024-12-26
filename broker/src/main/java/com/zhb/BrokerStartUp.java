package com.zhb;

import com.zhb.cache.CommonCache;
import com.zhb.config.GlobalPropertiesLoader;
import com.zhb.config.TopicInfoLoader;
import com.zhb.constants.BrokerConstants;
import com.zhb.core.MessageAppendHandler;
import com.zhb.model.MqTopicModel;

import java.io.IOException;

public class BrokerStartUp {

	private static GlobalPropertiesLoader globalPropertiesLoader;
	private static TopicInfoLoader topicInfoLoader;
	private static MessageAppendHandler messageAppendHandler;

	/**
	 * 初始化配置逻辑
	 */
	private static void initProperties() throws IOException {
		globalPropertiesLoader = new GlobalPropertiesLoader();
		globalPropertiesLoader.loadProperties();

		topicInfoLoader = new TopicInfoLoader();
		topicInfoLoader.loadProperties();

		messageAppendHandler = new MessageAppendHandler();
		for (MqTopicModel topicModel : CommonCache.getTopicModelList()) {
			String topicName = topicModel.getTopic();
			String filePath = CommonCache.getGlobalProperties().getMqHome() +
				BrokerConstants.BASE_STORE_PATH +
				topicName +
				"\\00000000";
			messageAppendHandler.prepareMMapLoading(filePath, topicName);
		}
	}

	public static void main(String[] args) throws IOException {
		//加载配置 ，缓存对象的生成
		initProperties();
		//模拟初始化文件映射
		String topic = "order_cancel_topic";
		messageAppendHandler.appendMsg(topic, "this is a test content");
		messageAppendHandler.readMsg(topic);
		//D:\code\zhuohb\backend\studymq\broker\config\store\order_cancel_topic\00000000
		//D:\code\zhuohb\backend\studymq\broker\config\store\order_topic\00000000
	}
}
