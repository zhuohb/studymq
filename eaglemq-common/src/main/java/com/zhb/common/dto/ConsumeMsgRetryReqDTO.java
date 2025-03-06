package com.zhb.common.dto;

import java.util.List;

/**
 * @Author idea
 * @Date: Created at 2024/8/8
 * @Description
 */
public class ConsumeMsgRetryReqDTO extends BaseBrokerRemoteDTO{

    private List<ConsumeMsgRetryReqDetailDTO> consumeMsgRetryReqDetailDTOList;

    public List<ConsumeMsgRetryReqDetailDTO> getConsumeMsgRetryReqDetailDTOList() {
        return consumeMsgRetryReqDetailDTOList;
    }

    public void setConsumeMsgRetryReqDetailDTOList(List<ConsumeMsgRetryReqDetailDTO> consumeMsgRetryReqDetailDTOList) {
        this.consumeMsgRetryReqDetailDTOList = consumeMsgRetryReqDetailDTOList;
    }
}
