package com.zhb.broker.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费队列内存映射文件模型管理器
 * 负责管理和维护所有主题的消费队列内存映射文件模型列表
 * 每个主题可能有多个队列，每个队列对应一个ConsumeQueueMMapFileModel
 */
public class ConsumeQueueMMapFileModelManager {

	/**
	 * 主题与消费队列映射文件模型列表的映射关系
	 * key: 主题名称
	 * value: 该主题下所有队列的消费队列内存映射文件模型列表
	 */
	public Map<String, List<ConsumeQueueMMapFileModel>> consumeQueueMMapFileModel = new HashMap<>();

	/**
	 * 存储指定主题的所有消费队列内存映射文件模型列表
	 *
	 * @param topic                      主题名称
	 * @param consumeQueueMMapFileModels 对应主题的所��队列的消费队列内存映射文件模型列表
	 */
	public void put(String topic, List<ConsumeQueueMMapFileModel> consumeQueueMMapFileModels) {
		consumeQueueMMapFileModel.put(topic, consumeQueueMMapFileModels);
	}

	/**
	 * 获取指定主题的所有消费队列内存映射文件模型列表
	 *
	 * @param topic 主题名称
	 * @return 对应主题的所有队列的消费队列内存映射文件模型列表，如果不存��则返回null
	 */
	public List<ConsumeQueueMMapFileModel> get(String topic) {
		return consumeQueueMMapFileModel.get(topic);
	}
}
