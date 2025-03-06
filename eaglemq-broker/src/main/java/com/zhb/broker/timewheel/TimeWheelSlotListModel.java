package com.zhb.broker.timewheel;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description
 */
public class TimeWheelSlotListModel {

    private List<TimeWheelSlotModel> timeWheelSlotModels = new LinkedList<>();

    public List<TimeWheelSlotModel> getTimeWheelSlotModels() {
        return timeWheelSlotModels;
    }

    public void setTimeWheelSlotModels(List<TimeWheelSlotModel> timeWheelSlotModels) {
        this.timeWheelSlotModels = timeWheelSlotModels;
    }
}
