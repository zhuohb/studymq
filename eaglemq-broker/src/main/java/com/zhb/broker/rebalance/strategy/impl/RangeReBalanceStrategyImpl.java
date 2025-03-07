package com.zhb.broker.rebalance.strategy.impl;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.broker.rebalance.strategy.IReBalanceStrategy;
import com.zhb.broker.rebalance.strategy.ReBalanceInfo;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 平均分配重平衡策略实现类
 * 负责在消费者实例之间按范围平均分配消息队列
 * 确保队列资源在消费者之间尽可能均匀分布
 */
public class RangeReBalanceStrategyImpl implements IReBalanceStrategy {

	/**
	 * 执行重平衡逻辑
	 * 按照范围策略将主题的队列平均分配给各个消费组中的消费者实例
	 * 确保每个消费者实例分配到尽量均等数量的队列
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
			if (CollectionUtils.isEmpty(consumerInstances)) {
				// 无消费者实例，跳过处理
				continue;
			}

			// 按消费组对消费者实例进行分组
			Map<String, List<ConsumerInstance>> consumeGroupMap = consumerInstances.stream()
				.collect(Collectors.groupingBy(ConsumerInstance::getConsumeGroup));

			// 获取主题模型和队列数量
			EagleMqTopicModel eagleMqTopicModel = eagleMqTopicModelMap.get(topic);
			int queueSize = eagleMqTopicModel.getQueueList().size();

			// 遍历所有消费组进行平均分配
			for (String consumerGroup : consumeGroupMap.keySet()) {
				List<ConsumerInstance> consumerInstanceList = consumeGroupMap.get(consumerGroup);

				// 计算每个消费者应该分配的队列数量
				int eachConsumerQueueNum = queueSize / consumerInstanceList.size();
				int queueId = 0;

				// 按顺序为每个消费者分配其应得的队列数量
				for (int i = 0; i < consumerInstanceList.size(); i++) {
					for (int queueNums = 0; queueNums < eachConsumerQueueNum; queueNums++) {
						consumerInstanceList.get(i).getQueueIdSet().add(queueId++);
					}
				}

				// 处理余下的队列：如果队列总数不能被消费者数量整除，则将余下的队列分配给前面的消费者
				int remainQueueCount = queueSize - queueId;
				if (remainQueueCount > 0) {
					for (int i = 0; i < remainQueueCount; i++) {
						consumerInstanceList.get(i).getQueueIdSet().add(queueId++);
					}
				}
			}
		}
	}
}
