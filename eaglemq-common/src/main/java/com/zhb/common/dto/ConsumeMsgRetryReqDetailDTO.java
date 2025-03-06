package com.zhb.common.dto;

/**
 * @Author idea
 * @Date: Created at 2024/7/21
 * @Description
 */
public class ConsumeMsgRetryReqDetailDTO {

    private String topic;
    private String consumerGroup;
    private Integer queueId;
    private String ip;
    private Integer port;
    private Long commitLogOffset;
    private Integer commitLogMsgLength;
//    private List<Long> commitLogOffsetList;
//    private List<Integer> commitLogMsgLengthList;
    private String commitLogName;
    //重试次数
    private int retryTime;


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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

//    public List<Long> getCommitLogOffsetList() {
//        return commitLogOffsetList;
//    }
//
//    public void setCommitLogOffsetList(List<Long> commitLogOffsetList) {
//        this.commitLogOffsetList = commitLogOffsetList;
//    }

    public String getCommitLogName() {
        return commitLogName;
    }

    public void setCommitLogName(String commitLogName) {
        this.commitLogName = commitLogName;
    }

//    public List<Integer> getCommitLogMsgLengthList() {
//        return commitLogMsgLengthList;
//    }
//
//    public void setCommitLogMsgLengthList(List<Integer> commitLogMsgLengthList) {
//        this.commitLogMsgLengthList = commitLogMsgLengthList;
//    }


    public Long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(Long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public Integer getCommitLogMsgLength() {
        return commitLogMsgLength;
    }

    public void setCommitLogMsgLength(Integer commitLogMsgLength) {
        this.commitLogMsgLength = commitLogMsgLength;
    }

    public int getRetryTime() {
        return retryTime;
    }

    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }
}
