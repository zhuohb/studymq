package com.zhb.broker.timewheel;

import lombok.Getter;

/**
 * 时间轮存储的槽所使用的时间单位
 */
@Getter
public enum TimeWheelSlotStepUnitEnum {

	SECOND("second"),
	MINUTE("minute"),
	HOUR("hour"),
	;

	TimeWheelSlotStepUnitEnum(String code) {
		this.code = code;
	}

	final String code;
}
