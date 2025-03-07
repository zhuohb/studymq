package com.zhb.broker.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
public class ConsumeQueueOffsetModel {

	private OffsetTable offsetTable = new OffsetTable();

	@Setter
	@Getter
	public static class OffsetTable {
		private Map<String, ConsumerGroupDetail> topicConsumerGroupDetail = new HashMap<>();
	}

	@Setter
	@Getter
	public static class ConsumerGroupDetail {
		private Map<String, Map<String, String>> consumerGroupDetailMap = new HashMap<>();
	}

}
