package com.zhb.broker.rebalance;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.rebalance.strategy.IReBalanceStrategy;
import com.zhb.broker.rebalance.strategy.ReBalanceInfo;
import com.zhb.broker.rebalance.strategy.impl.RandomReBalanceStrategyImpl;
import com.zhb.common.utils.AssertUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 消费者实例池
 * 管理全部消费者实例的注册、移除及消费队列的重平衡分配
 * 通过定时任务确保消费队列在消费者组中的负载均衡
 */
@Slf4j
public class ConsumerInstancePool {

	/**
	 * 消费者实例映射表
	 * 以主题(topic)为键，对应的消费者实例列表为值
	 */
	private final Map<String, List<ConsumerInstance>> consumeInstanceMap = new ConcurrentHashMap<>();

	/**
	 * 重平衡策略映射表
	 * 以策略名称为键，对应的重平衡策略实现为值
	 */
	private static final Map<String, IReBalanceStrategy> reBalanceStrategyMap = new HashMap<>();

	/**
	 * 重平衡信息对象
	 * 包含重平衡过程中需要的数据和状态
	 */
	private final ReBalanceInfo reBalanceInfo = new ReBalanceInfo();

	static {
		//静态初始化块 注册支持的重平衡策略实现
		reBalanceStrategyMap.put("random", new RandomReBalanceStrategyImpl());
		reBalanceStrategyMap.put("range", new RandomReBalanceStrategyImpl());
	}

	/**
	 * 将消费者实例添加到实例池
	 * 同时更新需要重平衡的消费组信息
	 *
	 * @param consumerInstance 需要添加的消费者实例
	 */
	public void addInstancePool(ConsumerInstance consumerInstance) {
		synchronized (this) {
			String topic = consumerInstance.getTopic();
			// 校验topic是否合法
			EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
			AssertUtils.isNotNull(eagleMqTopicModel, "topic非法");

			// 获取或创建该主题的消费者实例列表
			List<ConsumerInstance> consumerInstanceList = consumeInstanceMap.getOrDefault(topic, new ArrayList<>());

			// 检查是否已存在相同ID的消费者实例，避免重复添加
			for (ConsumerInstance instance : consumerInstanceList) {
				if (instance.getConsumerReqId().equals(consumerInstance.getConsumerReqId())) {
					return;
				}
			}

			// 添加新的消费者实例并更新映射
			consumerInstanceList.add(consumerInstance);
			consumeInstanceMap.put(topic, consumerInstanceList);

			// 更新需要重平衡的消费组信息
			Set<String> consumerGroupSet = reBalanceInfo.getChangeConsumerGroupMap().get(topic);
			if (CollectionUtils.isEmpty(consumerGroupSet)) {
				consumerGroupSet = new HashSet<>();
			}
			consumerGroupSet.add(consumerInstance.getConsumeGroup());
			reBalanceInfo.getChangeConsumerGroupMap().put(topic, consumerGroupSet);
			log.info("new instance add in pool:{}", JSON.toJSONString(consumerInstance));
		}
	}

	/**
	 * 从实例池中移除指定请求ID的消费者实例
	 * 更新消费持有映射表中的消费者列表
	 *
	 * @param reqId 消费者请求ID，通常是"IP:PORT"格式
	 */
	public void removeFromInstancePool(String reqId) {
		synchronized (this) {
			// 从消费实例池中移除匹配的消费者
			for (String topic : consumeInstanceMap.keySet()) {
				List<ConsumerInstance> consumerInstances = consumeInstanceMap.get(topic);
				// 过滤出不匹配给定请求ID的消费者实例
				List<ConsumerInstance> filterInstances = consumerInstances.stream()
					.filter(item -> !item.getConsumerReqId().equals(reqId))
					.collect(Collectors.toList());
				// 更新过滤后的消费者实例列表
				consumeInstanceMap.put(topic, filterInstances);
			}

			// 从消费持有映射中移除匹配的消费者
			Map<String, Map<String, List<ConsumerInstance>>> consumeHoldMap = CommonCache.getConsumeHoldMap();
			for (String topic : consumeHoldMap.keySet()) {
				Map<String, List<ConsumerInstance>> consumeGroupInstanceMap = consumeHoldMap.get(topic);
				for (String consumeGroup : consumeGroupInstanceMap.keySet()) {
					List<ConsumerInstance> consumerInstances = consumeGroupInstanceMap.get(consumeGroup);
					// 过滤出不匹配给定请求ID的消费者实例
					List<ConsumerInstance> filterInstances = consumerInstances.stream()
						.filter(item -> !item.getConsumerReqId().equals(reqId))
						.collect(Collectors.toList());
					consumeGroupInstanceMap.put(consumeGroup, filterInstances);
				}
			}
		}
	}

	/**
	 * 执行消费队列重平衡逻辑
	 * 根据配置的重平衡策略，重新分配队列给消费者
	 */
	public void doReBalance() {
		synchronized (this) {
			// 获取配置的重平衡策略
			String reBalanceStrategy = CommonCache.getGlobalProperties().getReBalanceStrategy();
			// 设置当前消费实例映射到重平衡信息对象
			reBalanceInfo.setConsumeInstanceMap(this.consumeInstanceMap);
			// 执行重平衡策略
			reBalanceStrategyMap.get(reBalanceStrategy).doReBalance(reBalanceInfo);
			// 清空已处理的变更消费组信息
			reBalanceInfo.getChangeConsumerGroupMap().clear();
		}
	}

	/**
	 * 启动重平衡定时任务
	 * 创建一个后台线程，定期执行重平衡逻辑
	 */
	public void startReBalanceJob() {
		Thread reBalanceTask = new Thread(() -> {
			while (true) {
				try {
					// 每10秒执行一次重平衡
					TimeUnit.SECONDS.sleep(10);
					doReBalance();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		reBalanceTask.setName("reBalance-task");
		reBalanceTask.start();
	}
}
