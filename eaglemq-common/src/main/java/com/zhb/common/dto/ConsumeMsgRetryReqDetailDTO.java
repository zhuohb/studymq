package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ConsumeMsgRetryReqDetailDTO {

	private String topic;
	private String consumerGroup;
	private Integer queueId;
	private String ip;
	private Integer port;
	private Long commitLogOffset;
	private Integer commitLogMsgLength;
	private String commitLogName;
	//重试次数
	private int retryTime;

}
