package com.zhb.broker.event.model;

import com.zhb.broker.timewheel.TimeWheelSlotModel;
import com.zhb.common.event.model.Event;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * 时间轮到期事件
 */
@Setter
@Getter
public class TimeWheelEvent extends Event {

    private List<TimeWheelSlotModel> timeWheelSlotModelList;

}
