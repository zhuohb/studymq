package com.zhb.common.dto;

import java.util.List;

/**
 * @author idea
 * @create 2024/6/25 23:35
 * @description
 */
public class ConsumeMsgBaseRespDTO extends BaseBrokerRemoteDTO{

    private List<ConsumeMsgRespDTO> consumeMsgRespDTOList;

    public List<ConsumeMsgRespDTO> getConsumeMsgRespDTOList() {
        return consumeMsgRespDTOList;
    }

    public void setConsumeMsgRespDTOList(List<ConsumeMsgRespDTO> consumeMsgRespDTOList) {
        this.consumeMsgRespDTOList = consumeMsgRespDTOList;
    }
}
