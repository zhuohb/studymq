package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;


@Setter
@Getter
public class ConsumeMsgRetryReqDTO extends BaseBrokerRemoteDTO{

    private List<ConsumeMsgRetryReqDetailDTO> consumeMsgRetryReqDetailDTOList;

}
