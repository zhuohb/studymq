package com.zhb.broker.timewheel;

import com.zhb.common.dto.MessageDTO;
import com.zhb.common.dto.MessageRetryDTO;
import com.zhb.common.dto.TxMessageDTO;
import lombok.Getter;

@Getter
public enum SlotStoreTypeEnum {

    MESSAGE_RETRY_DTO(MessageRetryDTO.class),
    DELAY_MESSAGE_DTO(MessageDTO.class),
    TX_MESSAGE_DTO(TxMessageDTO.class),
    ;
    Class clazz;

    SlotStoreTypeEnum(Class clazz) {
        this.clazz = clazz;
    }

}
