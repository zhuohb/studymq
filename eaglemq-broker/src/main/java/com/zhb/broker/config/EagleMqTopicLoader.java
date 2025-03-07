package com.zhb.broker.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.utils.FileContentUtil;
import com.zhb.common.constants.BrokerConstants;
import io.netty.util.internal.StringUtil;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 负责将mq的主题配置信息加载到内存中
 * 并定期将内存中的主题信息持久化到磁盘
 */
public class EagleMqTopicLoader {

	/**
	 * 主题配置文件的路径
	 */
	private String filePath;

	/**
	 * 从配置文件加载主题信息到内存中
	 * 读取JSON格式的配置文件，解析为主题模型列表，并存入公共缓存
	 */
	public void loadProperties() {
		// 获取全局配置中的基础路径
		GlobalProperties globalProperties = CommonCache.getGlobalProperties();
		String basePath = globalProperties.getEagleMqHome();
		if (StringUtil.isNullOrEmpty(basePath)) {
			throw new IllegalArgumentException("EAGLE_MQ_HOME is invalid!");
		}
		// 构建主题配置文件的完整路径
		filePath = basePath + "/config/eaglemq-topic.json";
		// 读取文件内容
		String fileContent = FileContentUtil.readFromFile(filePath);
		// 将JSON内容解析为主题模型列表
		List<EagleMqTopicModel> eagleMqTopicModelList = JSON.parseArray(fileContent, EagleMqTopicModel.class);
		// 存入公共缓存
		CommonCache.setEagleMqTopicModelList(eagleMqTopicModelList);
	}

	/**
	 * 开启一个刷新内存到磁盘的任务
	 */
	public void startRefreshEagleMqTopicInfoTask() {
		//异步线程
		//每3秒将内存中的配置刷新到磁盘里面
		CommonThreadPoolConfig.refreshEagleMqTopicExecutor.execute(() -> {
			try {
				do {
					try {
						TimeUnit.SECONDS.sleep(BrokerConstants.DEFAULT_REFRESH_MQ_TOPIC_TIME_STEP);
						// 从缓存获取最新的主题信息
						List<EagleMqTopicModel> eagleMqTopicModelList = CommonCache.getEagleMqTopicModelList();
						// 将主题信息以JSON格式写入文件，使用美化格式便于阅读
						FileContentUtil.overWriteToFile(filePath, JSON.toJSONString(eagleMqTopicModelList, SerializerFeature.PrettyFormat));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				} while (true);
			} catch (RuntimeException e) {
				throw new RuntimeException(e);
			}
		});

	}
}
