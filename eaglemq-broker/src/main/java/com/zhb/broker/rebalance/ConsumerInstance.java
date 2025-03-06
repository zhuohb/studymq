package com.zhb.broker.rebalance;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author idea
 * @Date: Created in 08:43 2024/6/18
 * @Description 消费实例对象
 */
public class ConsumerInstance {

    private String ip;
    private Integer port;
    private String consumeGroup;
    private String topic;
    private Integer batchSize;
    private Set<Integer> queueIdSet = new HashSet<>();
    private String consumerReqId;


    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getConsumerReqId() {
        return consumerReqId;
    }

    public void setConsumerReqId(String consumerReqId) {
        this.consumerReqId = consumerReqId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getConsumeGroup() {
        return consumeGroup;
    }

    public void setConsumeGroup(String consumeGroup) {
        this.consumeGroup = consumeGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Set<Integer> getQueueIdSet() {
        return queueIdSet;
    }

    public void setQueueIdSet(Set<Integer> queueIdSet) {
        this.queueIdSet = queueIdSet;
    }
}
