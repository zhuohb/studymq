package com.zhb.common.enums;

/**
 * @Author idea
 * @Date: Created at 2024/7/7
 * @Description
 */
public enum BrokerRegistryRoleEnum {

    MASTER("master"),
    SLAVE("slave"),
    SINGLE("single"),
    ;

    String code;

    BrokerRegistryRoleEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static BrokerRegistryRoleEnum of(String code) {
        for (BrokerRegistryRoleEnum brokerRegistryEnum : BrokerRegistryRoleEnum.values()) {
            if (brokerRegistryEnum.getCode().equals(code)) {
                return brokerRegistryEnum;
            }
        }
        return null;
    }
}
