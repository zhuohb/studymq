package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class ConsumeMsgRespDTO {

    /**
     * 队列id
     */
    private Integer queueId;
    /**
     * 拉数据返回内容
     */
    private List<ConsumeMsgCommitLogDTO> commitLogContentList;


}
