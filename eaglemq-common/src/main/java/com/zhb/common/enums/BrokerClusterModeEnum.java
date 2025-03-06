package com.zhb.common.enums;

/**
 * @Author idea
 * @Date: Created at 2024/7/7
 * @Description
 */
public enum BrokerClusterModeEnum {

    MASTER_SLAVE("master-slave"),
    ;
    private String code;

    BrokerClusterModeEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
