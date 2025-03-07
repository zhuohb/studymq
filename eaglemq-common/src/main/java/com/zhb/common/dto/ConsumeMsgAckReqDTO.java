package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * 消费消息返回ack信号DTO
 */
@Setter
@Getter
public class ConsumeMsgAckReqDTO extends BaseBrokerRemoteDTO {

	private String topic;
	private String consumeGroup;
	private Integer queueId;
	private Integer ackCount;
	private String ip;
	private Integer port;

}
