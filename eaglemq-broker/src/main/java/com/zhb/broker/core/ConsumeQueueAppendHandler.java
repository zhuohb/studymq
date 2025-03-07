package com.zhb.broker.core;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.model.QueueModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 消费队列追加处理器
 * 负责为指定主题准备和初始化消费队列的内存映射文件
 * 消费队列存储了消息在CommitLog中的位置索引，便于消费者快速消费
 */
public class ConsumeQueueAppendHandler {
	/**
	 * 准备指定主题的消费队列
	 * 为主题下的每个队列创建内存映射文件模型，并加载到内存中
	 *
	 * @param topicName 主题名称
	 * @throws IOException 如果文件操作失败
	 */
	public void prepareConsumeQueue(String topicName) throws IOException {
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topicName);
		List<QueueModel> queueModelList = eagleMqTopicModel.getQueueList();
		//retry重试topic没有consume queue存在
		if (queueModelList == null) {
			return;
		}
		List<ConsumeQueueMMapFileModel> consumeQueueMMapFileModels = new ArrayList<>();
		//循环遍历，mmap的初始化
		for (QueueModel queueModel : queueModelList) {
			ConsumeQueueMMapFileModel consumeQueueMMapFileModel = new ConsumeQueueMMapFileModel();
			consumeQueueMMapFileModel.loadFileInMMap(
				topicName,
				queueModel.getId(),
				queueModel.getLastOffset(),
				queueModel.getLatestOffset().get(),
				queueModel.getOffsetLimit());
			consumeQueueMMapFileModels.add(consumeQueueMMapFileModel);
		}
		// 将创建的消费队列内存映射文件模型列表存入公共缓存
		CommonCache.getConsumeQueueMMapFileModelManager().put(topicName, consumeQueueMMapFileModels);
	}
}
