package com.zhb.broker.rebalance;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

/**
 * 消费实例对象
 */
@Setter
@Getter
public class ConsumerInstance {

	private String ip;
	private Integer port;
	private String consumeGroup;
	private String topic;
	private Integer batchSize;
	private Set<Integer> queueIdSet = new HashSet<>();
	private String consumerReqId;


}
