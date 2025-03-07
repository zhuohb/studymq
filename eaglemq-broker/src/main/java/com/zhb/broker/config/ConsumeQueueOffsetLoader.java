package com.zhb.broker.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.ConsumeQueueOffsetModel;
import com.zhb.broker.utils.FileContentUtil;
import com.zhb.common.constants.BrokerConstants;
import io.netty.util.internal.StringUtil;

import java.util.concurrent.TimeUnit;

/**
 * 管理消费者偏移量的持久化
 */
public class ConsumeQueueOffsetLoader {

	private String filePath;

	/**
	 * 从磁盘加载消费者偏移量
	 */
	public void loadProperties() {
		GlobalProperties globalProperties = CommonCache.getGlobalProperties();
		String basePath = globalProperties.getEagleMqHome();
		if (StringUtil.isNullOrEmpty(basePath)) {
			throw new IllegalArgumentException("EAGLE_MQ_HOME is invalid!");
		}
		filePath = basePath + "/config/consumequeue-offset.json";
		String fileContent = FileContentUtil.readFromFile(filePath);
		ConsumeQueueOffsetModel consumeQueueOffsetModels = JSON.parseObject(fileContent, ConsumeQueueOffsetModel.class);
		CommonCache.setConsumeQueueOffsetModel(consumeQueueOffsetModels);
	}


	/**
	 * 开启一个刷新内存到磁盘的任务
	 */
	public void startRefreshConsumeQueueOffsetTask() {
		//异步线程
		//每3秒将内存中的配置刷新到磁盘里面
		CommonThreadPoolConfig.refreshConsumeQueueOffsetExecutor.execute(new Runnable() {
			@Override
			public void run() {
				do {
					try {
						TimeUnit.SECONDS.sleep(BrokerConstants.DEFAULT_REFRESH_CONSUME_QUEUE_OFFSET_TIME_STEP);
						ConsumeQueueOffsetModel consumeQueueOffsetModel = CommonCache.getConsumeQueueOffsetModel();
						FileContentUtil.overWriteToFile(filePath, JSON.toJSONString(consumeQueueOffsetModel, SerializerFeature.PrettyFormat));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				} while (true);
			}
		});

	}

}
