package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * 重试消息的底层存储结构
 */
@Setter
@Getter
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

}
