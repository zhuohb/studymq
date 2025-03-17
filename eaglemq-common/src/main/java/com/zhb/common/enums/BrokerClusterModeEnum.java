package com.zhb.common.enums;

import lombok.Getter;


@Getter
public enum BrokerClusterModeEnum {

    MASTER_SLAVE("master-slave"),
    ;
    private final String code;

    BrokerClusterModeEnum(String code) {
        this.code = code;
    }

}
