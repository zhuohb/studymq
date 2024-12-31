package com.zhb.model;

import lombok.Data;


@Data
public class QueueModel {

	private Integer id;
	private Long minOffset;
	private Long maxOffset;
	private Long currentOffset;

}
