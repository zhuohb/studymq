package com.zhb.broker.timewheel;

/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description 时间轮存储的槽所使用的时间单位
 */
public enum TimeWheelSlotStepUnitEnum {

    SECOND("second"),
    MINUTE("minute"),
    HOUR("hour"),
    ;

    TimeWheelSlotStepUnitEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    String code;
}
