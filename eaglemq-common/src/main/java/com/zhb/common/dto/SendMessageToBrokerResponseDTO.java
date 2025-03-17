package com.zhb.common.dto;

import com.zhb.common.enums.SendMessageToBrokerResponseStatus;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class SendMessageToBrokerResponseDTO extends BaseBrokerRemoteDTO {

	/**
	 * 发送消息的结果状态
	 *
	 * @see SendMessageToBrokerResponseStatus
	 */
	private int status;

	private String desc;

}
