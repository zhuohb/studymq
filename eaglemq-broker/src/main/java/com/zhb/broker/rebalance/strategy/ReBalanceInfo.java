package com.zhb.broker.rebalance.strategy;

import com.zhb.broker.rebalance.ConsumerInstance;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 需要参与重平衡的消费组
 */
@Setter
@Getter
public class ReBalanceInfo {

	private Map<String, List<ConsumerInstance>> consumeInstanceMap;

	//消费者发生变化的消费组
	private Map<String, Set<String>> changeConsumerGroupMap = new HashMap<>();

}
