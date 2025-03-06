package com.zhb.common.dto;

/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description 重试消息的底层存储结构
 */
public class MessageRetryDTO {

    /**
     * 重试的topic
     */
    private String topic;

    /**
     * 需要重试消费的消费组
     */
    private String consumeGroup;

    /**
     * 需要重试消费的队列
     */
    private int queueId;

    /**
     * 原始数据的commitlog地址
     */
    private int sourceCommitLogOffset;

    /**
     * 原始数据的commitlog长度
     */
    private int sourceCommitLogSize;

    /**
     * commitlog文件的名称 纯数字节省内存空间
     */
    private int commitLogName;

    /**
     * 下一次重试时间
     */
    private long nextRetryTime;

    /**
     * 重试次数
     */
    private int currentRetryTimes;

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

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getSourceCommitLogOffset() {
        return sourceCommitLogOffset;
    }

    public void setSourceCommitLogOffset(int sourceCommitLogOffset) {
        this.sourceCommitLogOffset = sourceCommitLogOffset;
    }

    public int getSourceCommitLogSize() {
        return sourceCommitLogSize;
    }

    public void setSourceCommitLogSize(int sourceCommitLogSize) {
        this.sourceCommitLogSize = sourceCommitLogSize;
    }

    public int getCommitLogName() {
        return commitLogName;
    }

    public void setCommitLogName(int commitLogName) {
        this.commitLogName = commitLogName;
    }

    public long getNextRetryTime() {
        return nextRetryTime;
    }

    public void setNextRetryTime(long nextRetryTime) {
        this.nextRetryTime = nextRetryTime;
    }

    public int getCurrentRetryTimes() {
        return currentRetryTimes;
    }

    public void setCurrentRetryTimes(int currentRetryTimes) {
        this.currentRetryTimes = currentRetryTimes;
    }
}
