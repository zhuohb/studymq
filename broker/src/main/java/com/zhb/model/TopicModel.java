package com.zhb.model;

import lombok.Data;

import java.util.List;

/**
 * topic映射对象
 */
@Data
public class TopicModel {
	private String topic;
	private CommitLogModel commitLogModel;
	private List<QueueModel> queueList;
	private Long createAt;
	private Long updateAt;
}
