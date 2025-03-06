package com.zhb.broker.rebalance.strategy.impl;

import org.apache.commons.collections4.CollectionUtils;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.rebalance.ConsumerInstance;
import com.zhb.broker.rebalance.strategy.IReBalanceStrategy;
import com.zhb.broker.rebalance.strategy.ReBalanceInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author idea
 * @Date: Created in 14:09 2024/6/23
 * @Description 平均分配
 */
public class RangeReBalanceStrategyImpl implements IReBalanceStrategy {

    @Override
    public void doReBalance(ReBalanceInfo reBalanceInfo) {
        Map<String, List<ConsumerInstance>> consumeInstanceMap = reBalanceInfo.getConsumeInstanceMap();
        Map<String, EagleMqTopicModel> eagleMqTopicModelMap = CommonCache.getEagleMqTopicModelMap();
        for (String topic : consumeInstanceMap.keySet()) {
            List<ConsumerInstance> consumerInstances = consumeInstanceMap.get(topic);
            if (CollectionUtils.isEmpty(consumerInstances)) {
                continue;
            }
            //每个消费组实例
            Map<String, List<ConsumerInstance>> consumeGroupMap = consumerInstances.stream().collect(Collectors.groupingBy(ConsumerInstance::getConsumeGroup));
            EagleMqTopicModel eagleMqTopicModel = eagleMqTopicModelMap.get(topic);
            int queueSize = eagleMqTopicModel.getQueueList().size();
            for (String consumerGroup : consumeGroupMap.keySet()) {
                List<ConsumerInstance> consumerInstanceList = consumeGroupMap.get(consumerGroup);
                //算出每个消费者平均拥有多少条队列
                int eachConsumerQueueNum = queueSize / consumerInstanceList.size();
                int queueId = 0;
                for (int i = 0; i < consumerInstanceList.size(); i++) {
                    for (int queueNums = 0; queueNums < eachConsumerQueueNum; queueNums++) {
                        consumerInstanceList.get(i).getQueueIdSet().add(queueId++);
                    }
                }
                //代表有多余队列没有被用到
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
