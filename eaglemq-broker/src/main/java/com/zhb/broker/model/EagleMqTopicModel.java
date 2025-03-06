package com.zhb.broker.model;

import java.util.List;

/**
 * @Author idea
 * @Date: Created in 22:52 2024/3/26
 * @Description mq的topic映射对象
 */
public class EagleMqTopicModel {

    private String topic;
    private CommitLogModel commitLogModel;
    private List<QueueModel> queueList;
    private Long createAt;
    private Long updateAt;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<QueueModel> getQueueList() {
        return queueList;
    }

    public void setQueueList(List<QueueModel> queueList) {
        this.queueList = queueList;
    }

    public Long getCreateAt() {
        return createAt;
    }

    public void setCreateAt(Long createAt) {
        this.createAt = createAt;
    }

    public Long getUpdateAt() {
        return updateAt;
    }

    public void setUpdateAt(Long updateAt) {
        this.updateAt = updateAt;
    }

    public CommitLogModel getCommitLogModel() {
        return commitLogModel;
    }

    public void setCommitLogModel(CommitLogModel commitLogModel) {
        this.commitLogModel = commitLogModel;
    }

    @Override
    public String toString() {
        return "EagleMqTopicModel{" +
                "topic='" + topic + '\'' +
                ", queueList=" + queueList +
                ", commitLogModel=" + commitLogModel +
                ", createAt=" + createAt +
                ", updateAt=" + updateAt +
                '}';
    }
}
