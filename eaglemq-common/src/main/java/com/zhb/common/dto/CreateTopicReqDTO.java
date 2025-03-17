package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * 创建topic请求
 */
@Setter
@Getter
public class CreateTopicReqDTO extends BaseBrokerRemoteDTO {

	private String topic;

	private Integer queueSize;

}
