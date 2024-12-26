package com.zhb.model;

import lombok.Data;

import java.util.List;

/**
 * topic映射对象
 */
@Data
public class MqTopicModel {
	private String topic;
	private List<QueueModel> queueList;
	private Long createAt;
	private Long updateAt;
}
