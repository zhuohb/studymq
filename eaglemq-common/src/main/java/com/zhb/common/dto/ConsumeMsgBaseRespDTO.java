package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;


@Setter
@Getter
public class ConsumeMsgBaseRespDTO extends BaseBrokerRemoteDTO {

	private List<ConsumeMsgRespDTO> consumeMsgRespDTOList;

}
