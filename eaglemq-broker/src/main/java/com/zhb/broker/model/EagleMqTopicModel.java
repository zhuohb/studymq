package com.zhb.broker.model;

import lombok.Data;

import java.util.List;

/**
 * mq的topic映射对象
 */
@Data
public class EagleMqTopicModel {

	private String topic;
	private CommitLogModel commitLogModel;
	private List<QueueModel> queueList;
	private Long createAt;
	private Long updateAt;
}
