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
 * @Author idea
 * @Date: Created in 14:07 2024/6/23
 * @Description 随机重平衡
 */
public class RandomReBalanceStrategyImpl implements IReBalanceStrategy {

    @Override
    public void doReBalance(ReBalanceInfo reBalanceInfo) {
        Map<String, List<ConsumerInstance>> consumeInstanceMap = reBalanceInfo.getConsumeInstanceMap();
        Map<String, EagleMqTopicModel> eagleMqTopicModelMap = CommonCache.getEagleMqTopicModelMap();
        for (String topic : consumeInstanceMap.keySet()) {
            //指定topic当下的所有消费者实例
            List<ConsumerInstance> consumerInstances = consumeInstanceMap.get(topic);
            EagleMqTopicModel eagleMqTopicModel = eagleMqTopicModelMap.get(topic);
            if (eagleMqTopicModel == null) {
                //异常topic
                continue;
            }
            Map<String, List<ConsumerInstance>> consumeGroupMap = consumerInstances.stream().collect(Collectors.groupingBy(ConsumerInstance::getConsumeGroup));
            //此时可能队列还没分配
            if (eagleMqTopicModel.getQueueList() == null) {
                continue;
            }
            //队列数
            int queueNum = eagleMqTopicModel.getQueueList().size();
            //取出当前topic有变更过的消费组名
            Set<String> changeConsumerGroup = reBalanceInfo.getChangeConsumerGroupMap().get(topic);
            if(changeConsumerGroup==null || changeConsumerGroup.isEmpty()) {
                //目前没有新消费者加入，不需要触发重平衡
                return;
            }
            Map<String, List<ConsumerInstance>> consumeGroupHoldMap = new ConcurrentHashMap<>();
            for (String consumeGroup : consumeGroupMap.keySet()) {
                //变更的消费组名单中没包含当前消费组，不触发重平衡
                if (!changeConsumerGroup.contains(consumeGroup)) {
                    //依旧保存之前的消费组信息
                    consumeGroupHoldMap.put(consumeGroup, consumeGroupMap.get(consumeGroup));
                    continue;
                }
                //当前消费组有变更
                List<ConsumerInstance> consumerGroupInstanceList = consumeGroupMap.get(consumeGroup);
                //先释放消费者之前所持有的队列，重新分配
                for (ConsumerInstance consumerInstance : consumerGroupInstanceList) {
                    consumerInstance.getQueueIdSet().clear();
                }
                List<ConsumerInstance> newConsumerQueueInstanceList = new ArrayList<>();
                int consumeNum = consumerGroupInstanceList.size();
                //队列数大于消费者个数，那么每个消费者都会有队列持有
                Collections.shuffle(consumerGroupInstanceList);
                //参与重平衡的消费者，需要重新分配队列
                if (queueNum >= consumeNum) {
                    int j = 0;
                    for (int i = 0; i < consumeNum; i++, j++) {
                        ConsumerInstance consumeInstance = consumerGroupInstanceList.get(i);
                        consumeInstance.getQueueIdSet().add(j);
                        newConsumerQueueInstanceList.add(consumeInstance);
                    }
                    for (; j < queueNum; j++) {
                        Random random = new Random();
                        int randomConsumerId = random.nextInt(consumeNum);
                        ConsumerInstance consumerInstance = consumerGroupInstanceList.get(randomConsumerId);
                        consumerInstance.getQueueIdSet().add(j);
                    }
                } else {
                    for (int i = 0; i < queueNum; i++) {
                        ConsumerInstance consumerInstance = consumerGroupInstanceList.get(i);
                        consumerInstance.getQueueIdSet().add(i);
                    }
                }
                consumeGroupHoldMap.put(consumeGroup, newConsumerQueueInstanceList);
            }
            CommonCache.getConsumeHoldMap().put(topic, consumeGroupHoldMap);
        }
    }


}
