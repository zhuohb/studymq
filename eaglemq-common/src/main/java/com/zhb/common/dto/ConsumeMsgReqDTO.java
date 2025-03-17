package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * 消费端拉数据请求DTO
 */
@Setter
@Getter
public class ConsumeMsgReqDTO extends BaseBrokerRemoteDTO {

	private String topic;
	private String consumeGroup;
	private String ip;
	private Integer port;
	private Integer batchSize;

}
