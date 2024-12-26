package com.zhb.model;

import lombok.Data;

/**
 * 消息队列映射对象
 */
@Data
public class QueueModel {
	private Integer id;
	private Long minOffset;
	private Long maxOffset;
	private Long currentOffset;
}
