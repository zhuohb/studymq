package com.zhb.broker.event.model;

import com.zhb.common.dto.CreateTopicReqDTO;
import com.zhb.common.event.model.Event;

/**
 * @author idea
 * @create 2024/7/3 08:07
 * @description 创建topic事件
 */
public class CreateTopicEvent extends Event {

    private CreateTopicReqDTO createTopicReqDTO;

    public CreateTopicReqDTO getCreateTopicReqDTO() {
        return createTopicReqDTO;
    }

    public void setCreateTopicReqDTO(CreateTopicReqDTO createTopicReqDTO) {
        this.createTopicReqDTO = createTopicReqDTO;
    }
}
