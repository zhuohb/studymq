package com.zhb.config;

import com.alibaba.fastjson.JSON;
import com.zhb.cache.CommonCache;
import com.zhb.constants.BrokerConstants;
import com.zhb.model.EagleMqTopicModel;
import com.zhb.utils.FileContentUtil;
import io.netty.util.internal.StringUtil;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 加载mq的topic的配置信息
 */
public class EagleMqTopicLoader {

	private String filePath;

	public void loadProperties() {
		GlobalProperties globalProperties = CommonCache.getGlobalProperties();
		String basePath = globalProperties.getEagleMqHome();
		if (StringUtil.isNullOrEmpty(basePath)) {
			throw new IllegalArgumentException("EAGLE_MQ_HOME is invalid!");
		}
		filePath = basePath + "/config/eaglemq-topic.json";
		String fileContent = FileContentUtil.readFromFile(filePath);
		List<EagleMqTopicModel> eagleMqTopicModelList = JSON.parseArray(fileContent, EagleMqTopicModel.class);
		CommonCache.setEagleMqTopicModelList(eagleMqTopicModelList);
	}

	/**
	 * 开启一个刷新内存到磁盘的任务
	 */
	public void startRefreshEagleMqTopicInfoTask() {
		//异步线程
		//每隔10秒将内存中的配置刷新到磁盘里面
		CommonThreadPoolConfig.refreshEagleMqTopicExecutor.execute(new Runnable() {
			@Override
			public void run() {
				do {
					try {
						TimeUnit.SECONDS.sleep(BrokerConstants.DEFAULT_REFRESH_MQ_TOPIC_TIME_STEP);
						System.out.println("刷新磁盘");
						List<EagleMqTopicModel> eagleMqTopicModelList = CommonCache.getEagleMqTopicModelList();
						FileContentUtil.overWriteToFile(filePath, JSON.toJSONString(eagleMqTopicModelList));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				} while (true);
			}
		});

	}
}
