package com.zhb.broker.event.model;

import com.zhb.broker.timewheel.TimeWheelSlotModel;
import com.zhb.common.event.model.Event;

import java.util.List;

/**
 * @Author idea
 * @Date: Created at 2024/8/4
 * @Description 时间轮到期事件
 */
public class TimeWheelEvent extends Event {

    private List<TimeWheelSlotModel> timeWheelSlotModelList;

    public List<TimeWheelSlotModel> getTimeWheelSlotModelList() {
        return timeWheelSlotModelList;
    }

    public void setTimeWheelSlotModelList(List<TimeWheelSlotModel> timeWheelSlotModelList) {
        this.timeWheelSlotModelList = timeWheelSlotModelList;
    }
}
