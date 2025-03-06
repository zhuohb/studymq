package com.zhb.broker.model;

/**
 * @Author idea
 * @Date: Created in 20:37 2024/6/23
 * @Description
 */
public class ConsumeQueueConsumeReqModel {

    private String topic;
    private String consumeGroup;
    private Integer queueId;
    private Integer batchSize;

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumeGroup() {
        return consumeGroup;
    }

    public void setConsumeGroup(String consumeGroup) {
        this.consumeGroup = consumeGroup;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }
}
