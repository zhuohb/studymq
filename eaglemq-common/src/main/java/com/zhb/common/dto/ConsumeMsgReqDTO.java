package com.zhb.common.dto;

/**
 * @Author idea
 * @Date: Created in 22:17 2024/6/19
 * @Description 消费端拉数据请求DTO
 */
public class ConsumeMsgReqDTO extends BaseBrokerRemoteDTO{

    private String topic;
    private String consumeGroup;
    private String ip;
    private Integer port;
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
}
