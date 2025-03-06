package com.zhb.broker.timewheel;

import com.zhb.common.utils.AssertUtils;

/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description 时间轮组件
 */
public class TimeWheelModel {

    private int current;
    private TimeWheelSlotListModel timeWheelSlotListModel[];
    /**
     * 时间轮的存储时间单位
     *
     * @see TimeWheelSlotStepUnitEnum
     */
    private String unit;


    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public TimeWheelSlotListModel[] getTimeWheelSlotListModel() {
        return timeWheelSlotListModel;
    }

    public void setTimeWheelSlotListModel(TimeWheelSlotListModel[] timeWheelSlotListModel) {
        this.timeWheelSlotListModel = timeWheelSlotListModel;
    }

    public int getCurrent() {
        return current;
    }

    public void setCurrent(int current) {
        this.current = current;
    }

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
