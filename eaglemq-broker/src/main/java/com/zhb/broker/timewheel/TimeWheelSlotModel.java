package com.zhb.broker.timewheel;


import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TimeWheelSlotModel {

	/**
	 * 需要延迟的秒数
	 */
	private int delaySeconds;

	/**
	 * 元数据
	 */
	private Object data;

	/**
	 * 存储类型
	 *
	 * @see SlotStoreTypeEnum
	 */
	private Class storeType;

	/**
	 * 下一次执行时间
	 */
	private long nextExecuteTime;

}
