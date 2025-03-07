package com.zhb.broker.model;


import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ConsumeQueueConsumeReqModel {

    private String topic;
    private String consumeGroup;
    private Integer queueId;
    private Integer batchSize;

}
