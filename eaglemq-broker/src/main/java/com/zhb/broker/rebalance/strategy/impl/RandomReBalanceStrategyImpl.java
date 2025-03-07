package com.zhb.broker.rebalance.strategy.impl;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.broker.rebalance.strategy.IReBalanceStrategy;
import com.zhb.broker.rebalance.strategy.ReBalanceInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 随机重平衡策略实现类
 * 负责在消费者实例之间随机分配消息队列
 * 确保在消费者数量变化时能够合理地重新分配队列资源
 */
public class RandomReBalanceStrategyImpl implements IReBalanceStrategy {

	/**
	 * 执行重平衡逻辑
	 * 根据消费组的变更情况，重新分配主题的队列给消费者实例
	 * 使用随机分配策略确保负载分布
	 *
	 * @param reBalanceInfo 重平衡信息对象，包含消费实例映射和变更的消费组信息
	 */
	@Override
	public void doReBalance(ReBalanceInfo reBalanceInfo) {
		// 获取消费实例映射和主题模型映射
		Map<String, List<ConsumerInstance>> consumeInstanceMap = reBalanceInfo.getConsumeInstanceMap();
		Map<String, EagleMqTopicModel> eagleMqTopicModelMap = CommonCache.getEagleMqTopicModelMap();

		// 遍历所有主题进行重平衡处理
		for (String topic : consumeInstanceMap.keySet()) {
			// 获取指定主题当下的所有消费者实例
			List<ConsumerInstance> consumerInstances = consumeInstanceMap.get(topic);
			EagleMqTopicModel eagleMqTopicModel = eagleMqTopicModelMap.get(topic);

			// 检查主题模型是否存在
			if (eagleMqTopicModel == null) {
				// 异常主题，跳过处理
				continue;
			}

			// 按消费组对消费者实例进行分组
			Map<String, List<ConsumerInstance>> consumeGroupMap = consumerInstances.stream()
				.collect(Collectors.groupingBy(ConsumerInstance::getConsumeGroup));

			// 检查队列是否已初始化
			if (eagleMqTopicModel.getQueueList() == null) {
				continue;
			}

			// 获取主题的队列数量
			int queueNum = eagleMqTopicModel.getQueueList().size();

			// 获取当前主题有���更过的消费组名称集合
			Set<String> changeConsumerGroup = reBalanceInfo.getChangeConsumerGroupMap().get(topic);

			// 检查是否有消费组需要重平衡
			if (changeConsumerGroup == null || changeConsumerGroup.isEmpty()) {
				// 目前没有新消费者加入，不需要触发重平衡
				return;
			}

			// 创建新的消费组持有映射表
			Map<String, List<ConsumerInstance>> consumeGroupHoldMap = new ConcurrentHashMap<>();

			// 遍历所有消费组进行重平衡处理
			for (String consumeGroup : consumeGroupMap.keySet()) {
				// 判断当前消费组是否在变更列表中
				if (!changeConsumerGroup.contains(consumeGroup)) {
					// 未变更的消费组保持原有队列分配
					consumeGroupHoldMap.put(consumeGroup, consumeGroupMap.get(consumeGroup));
					continue;
				}

				// 处理有变更的消费组
				List<ConsumerInstance> consumerGroupInstanceList = consumeGroupMap.get(consumeGroup);

				// 释放该消费组所有消费者实例持有的队列，准备重新分配
				for (ConsumerInstance consumerInstance : consumerGroupInstanceList) {
					consumerInstance.getQueueIdSet().clear();
				}

				// 创建重分配后的消费者实例列表
				List<ConsumerInstance> newConsumerQueueInstanceList = new ArrayList<>();
				int consumeNum = consumerGroupInstanceList.size();

				// 随机打乱消费者列表顺序，增加分配的随机性
				Collections.shuffle(consumerGroupInstanceList);

				// 根据队列数量和消费者数量的关系执行不同的分配逻辑
				if (queueNum >= consumeNum) {
					// 队列数大于等于消费者数，确保每个消费者至少分配一个队列
					int j = 0;
					for (int i = 0; i < consumeNum; i++, j++) {
						ConsumerInstance consumeInstance = consumerGroupInstanceList.get(i);
						consumeInstance.getQueueIdSet().add(j);
						newConsumerQueueInstanceList.add(consumeInstance);
					}

					// 剩余队列随机分配给消费者
					for (; j < queueNum; j++) {
						Random random = new Random();
						int randomConsumerId = random.nextInt(consumeNum);
						ConsumerInstance consumerInstance = consumerGroupInstanceList.get(randomConsumerId);
						consumerInstance.getQueueIdSet().add(j);
					}
				} else {
					// 队列数少于消费者数，只有部分消费者能获得队列
					for (int i = 0; i < queueNum; i++) {
						ConsumerInstance consumerInstance = consumerGroupInstanceList.get(i);
						consumerInstance.getQueueIdSet().add(i);
					}
				}

				// 更新消费组的队列分配结果
				consumeGroupHoldMap.put(consumeGroup, newConsumerQueueInstanceList);
			}

			// 更新全局缓存中的消费持有映射
			CommonCache.getConsumeHoldMap().put(topic, consumeGroupHoldMap);
		}
	}
}
