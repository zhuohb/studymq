package com.zhb.broker.model;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

@Setter
@Getter
public class QueueModel {

	private Integer id;
	private String fileName;
	private Integer offsetLimit;
	private AtomicInteger latestOffset;
	private Integer lastOffset;

	public int countDiff() {
		return this.getOffsetLimit() - this.getLatestOffset().get();
	}
}
