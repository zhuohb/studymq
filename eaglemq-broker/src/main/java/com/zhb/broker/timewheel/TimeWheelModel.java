package com.zhb.broker.timewheel;

import com.zhb.common.utils.AssertUtils;
import lombok.Getter;
import lombok.Setter;

/**
 * 时间轮组件
 */
@Setter
@Getter
public class TimeWheelModel {

	private int current;
	private TimeWheelSlotListModel timeWheelSlotListModel[];
	/**
	 * 时间轮的存储时间单位
	 *
	 * @see TimeWheelSlotStepUnitEnum
	 */
	private String unit;


	public int countNextSlot(int delay) {
		AssertUtils.isTrue(delay < timeWheelSlotListModel.length, "delay can not large than slot's total count");
		int remainSlotCount = timeWheelSlotListModel.length - current;
		int diff = delay - remainSlotCount;
		if (diff < 0) {
			return current + delay;
		}
		return diff;
	}


}
