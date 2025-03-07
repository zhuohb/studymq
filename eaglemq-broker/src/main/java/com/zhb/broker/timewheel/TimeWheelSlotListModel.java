package com.zhb.broker.timewheel;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

@Setter
@Getter
public class TimeWheelSlotListModel {

	private List<TimeWheelSlotModel> timeWheelSlotModels = new LinkedList<>();

}
