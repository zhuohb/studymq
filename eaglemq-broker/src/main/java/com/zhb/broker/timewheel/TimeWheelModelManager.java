package com.zhb.broker.timewheel;


import com.zhb.broker.event.model.TimeWheelEvent;
import com.zhb.common.event.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 时间轮模型管理器
 * 负责管理秒级和分钟级两个层次的时间轮
 * 处理延迟消息的添加、扫描和执行
 */
@Slf4j
public class TimeWheelModelManager {

	/**
	 * 秒级时间轮同步锁
	 */
	private final Object secondsLock = new Object();

	/**
	 * 分钟级时间轮同步锁
	 */
	private final Object minutesLock = new Object();

	/**
	 * 已执行的秒数计数器
	 */
	private long executeSeconds = 0l;

	/**
	 * 秒级时间轮模型
	 */
	private TimeWheelModel secondsTimeWheelModel;

	/**
	 * 分钟级时间轮模型
	 */
	private TimeWheelModel minutesTimeWheelModel;

	/**
	 * 事件总线，用于发布时间轮触发的事件
	 */
	private EventBus eventBus;


	/**
	 * 初始化时间轮内部的变量值
	 * 创建秒级和分钟级两个时间轮，并初始化事件总线
	 *
	 * @param eventBus 事件总线实例
	 */
	public void init(EventBus eventBus) {
		// 初始化秒级时间轮，包含60个槽位
		secondsTimeWheelModel = new TimeWheelModel();
		secondsTimeWheelModel.setUnit(TimeWheelSlotStepUnitEnum.SECOND.getCode());
		secondsTimeWheelModel.setCurrent(0);
		secondsTimeWheelModel.setTimeWheelSlotListModel(buildTimeWheelSlotListModel(60));

		// 初始化分钟级时间轮，包含60个槽位
		minutesTimeWheelModel = new TimeWheelModel();
		minutesTimeWheelModel.setCurrent(0);
		minutesTimeWheelModel.setTimeWheelSlotListModel(buildTimeWheelSlotListModel(60));
		minutesTimeWheelModel.setUnit(TimeWheelSlotStepUnitEnum.MINUTE.getCode());

		// 设置并初始化事件总线
		this.eventBus = eventBus;
		this.eventBus.init();
	}

	/**
	 * 添加延迟消息到时间轮
	 * 根据延迟时间长短决定放入秒级还是分钟级时间轮
	 *
	 * @param delayMessageDTO 延迟消息数据传输对象
	 */
	public void add(DelayMessageDTO delayMessageDTO) {
		int delay = delayMessageDTO.getDelay();
		// 计算延迟的分钟数
		int min = delay / 60;

		if (min == 0) {
			// 延迟小于1分钟，放入秒级时间轮
			synchronized (secondsLock) {
				// 计算应放入的槽位
				int nextSlot = secondsTimeWheelModel.countNextSlot(delay);
				log.info("current second slot:{},next slot:{}", secondsTimeWheelModel.getCurrent(), nextSlot);

				// 获取对应槽位并创建时间轮槽位模型
				TimeWheelSlotListModel timeWheelSlotListModel = secondsTimeWheelModel.getTimeWheelSlotListModel()[nextSlot];
				TimeWheelSlotModel timeWheelSlotModel = new TimeWheelSlotModel();
				timeWheelSlotModel.setData(delayMessageDTO.getData());
				timeWheelSlotModel.setDelaySeconds(delayMessageDTO.getDelay());
				timeWheelSlotModel.setNextExecuteTime(delayMessageDTO.getNextExecuteTime());
				timeWheelSlotModel.setStoreType(delayMessageDTO.getSlotStoreType().getClazz());

				// 将任务添加到对应槽位
				timeWheelSlotListModel.getTimeWheelSlotModels().add(timeWheelSlotModel);
			}
		} else if (min > 0) {
			// 延迟大于等于1分钟，放入分钟级时间轮
			synchronized (minutesLock) {
				// 计算应放入的槽位
				int nextSlot = minutesTimeWheelModel.countNextSlot(min);
				log.info("current minutes slot:{},next slot:{}", minutesTimeWheelModel.getCurrent(), nextSlot);

				// 获取对应槽位并创建时间轮槽位模型
				TimeWheelSlotListModel timeWheelSlotListModel = minutesTimeWheelModel.getTimeWheelSlotListModel()[nextSlot];
				TimeWheelSlotModel timeWheelSlotModel = new TimeWheelSlotModel();
				timeWheelSlotModel.setData(delayMessageDTO.getData());
				timeWheelSlotModel.setDelaySeconds(delayMessageDTO.getDelay());
				timeWheelSlotModel.setNextExecuteTime(delayMessageDTO.getNextExecuteTime());
				timeWheelSlotModel.setStoreType(delayMessageDTO.getSlotStoreType().getClazz());

				// 将任务添加到对应槽位
				timeWheelSlotListModel.getTimeWheelSlotModels().add(timeWheelSlotModel);
			}
		}
	}

	/**
	 * 开启扫描slot数组任务
	 * 启动一个单独线程，按秒为单位扫描时间轮
	 * 触发到期任务的执行
	 */
	public void doScanTask() {
		Thread scanThread = new Thread(() -> {
			log.info("start scan slot task");
			while (true) {
				try {
					// 执行秒级时间轮的扫描
					doSecondsTimeWheelExecute();

					// 每60秒执行一次分钟级时间轮的扫描
					if (executeSeconds % 60 == 0) {
						doMinutesTimeWheelExecute();
					}

					// 休眠1秒
					TimeUnit.SECONDS.sleep(1);
					executeSeconds++;
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		});
		scanThread.setName("scan-slot-task");
		scanThread.start();
	}

	/**
	 * 执行秒级时间轮的检查和任务触发
	 * 发布当前时间��中的所有任务事件
	 */
	private void doSecondsTimeWheelExecute() {
		synchronized (secondsLock) {
			// 获取当前槽位和其中的任务列表
			int current = secondsTimeWheelModel.getCurrent();
			TimeWheelSlotListModel timeWheelSlotListModel = secondsTimeWheelModel.getTimeWheelSlotListModel()[current];
			List<TimeWheelSlotModel> timeWheelSlotModelList = timeWheelSlotListModel.getTimeWheelSlotModels();

			// 如果当前槽位有任务，则发布事件触发执行
			if (CollectionUtils.isNotEmpty(timeWheelSlotModelList)) {
				TimeWheelEvent timeWheelEvent = new TimeWheelEvent();
				List<TimeWheelSlotModel> copySlotModel = timeWheelSlotModelList;
				timeWheelEvent.setTimeWheelSlotModelList(copySlotModel);
				eventBus.publish(timeWheelEvent);
			}

			// 执行完成后清空当前槽位
			timeWheelSlotListModel.setTimeWheelSlotModels(new ArrayList<>());

			// 更新当前槽位指针，循环使用时间轮
			if (current == secondsTimeWheelModel.getTimeWheelSlotListModel().length - 1) {
				current = 0;
			} else {
				current = current + 1;
			}
			secondsTimeWheelModel.setCurrent(current);
		}
	}

	/**
	 * 执行分钟级时间轮的检查和任务触发
	 * 将分钟级任务转换为秒级任务，或直接触发执行
	 */
	private void doMinutesTimeWheelExecute() {
		synchronized (minutesLock) {
			log.info("doScan minutes slot:{}", minutesTimeWheelModel.getCurrent());

			// 获取当前槽位和其中的任务列表
			int current = minutesTimeWheelModel.getCurrent();
			TimeWheelSlotListModel timeWheelSlotListModel = minutesTimeWheelModel.getTimeWheelSlotListModel()[current];
			List<TimeWheelSlotModel> timeWheelSlotModelList = timeWheelSlotListModel.getTimeWheelSlotModels();
			List<TimeWheelSlotModel> minutesTimeWheelModelList = new ArrayList<>();

			// 处理每个任务
			for (TimeWheelSlotModel timeWheelSlotModel : timeWheelSlotModelList) {
				// 计算剩余秒数
				int remainSecond = timeWheelSlotModel.getDelaySeconds() % 60;

				// 如果延迟的时间不是分钟的整数（如2min30s）
				if (remainSecond > 0) {
					// 需要被转移到秒级别的时间轮继续等待
					DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
					delayMessageDTO.setDelay(remainSecond);
					delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.MESSAGE_RETRY_DTO);
					delayMessageDTO.setData(timeWheelSlotModel.getData());
					delayMessageDTO.setNextExecuteTime(timeWheelSlotModel.getNextExecuteTime());
					add(delayMessageDTO);
					log.info("added message to second timeWheel");
				} else {
					// 整分钟任务直接加入执行列表
					minutesTimeWheelModelList.add(timeWheelSlotModel);
				}
			}

			// 如果有整分钟任务，直接发布事件触发执行
			if (CollectionUtils.isNotEmpty(minutesTimeWheelModelList)) {
				TimeWheelEvent timeWheelEvent = new TimeWheelEvent();
				timeWheelEvent.setTimeWheelSlotModelList(timeWheelSlotModelList);
				eventBus.publish(timeWheelEvent);
			}

			// 执行完成后清空当前槽位
			timeWheelSlotListModel.setTimeWheelSlotModels(new ArrayList<>());

			// 更新当前槽位指针，循环使用时间轮
			if (current == minutesTimeWheelModel.getTimeWheelSlotListModel().length - 1) {
				current = 0;
			} else {
				current = current + 1;
			}
			minutesTimeWheelModel.setCurrent(current);
		}
	}

	/**
	 * 构建指定数量的时间轮槽位列表模型
	 *
	 * @param count 槽位数量
	 * @return 初始化的时间轮槽位列表数组
	 */
	private TimeWheelSlotListModel[] buildTimeWheelSlotListModel(int count) {
		TimeWheelSlotListModel[] timeWheelSlotListModels = new TimeWheelSlotListModel[count];
		for (int i = 0; i < count; i++) {
			timeWheelSlotListModels[i] = new TimeWheelSlotListModel();
		}
		return timeWheelSlotListModels;
	}
}
