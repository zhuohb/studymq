package com.zhb.common.enums;

/**
 * @Author idea
 * @Date: Created in 19:50 2024/6/10
 * @Description 注册类型
 */
public enum RegistryTypeEnum {

    PRODUCER("producer"),
    CONSUMER("consumer"),
    BROKER("broker")
    ;
    String code;

    RegistryTypeEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
