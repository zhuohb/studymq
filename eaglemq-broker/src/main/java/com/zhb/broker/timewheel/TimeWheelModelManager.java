package com.zhb.broker.timewheel;


import org.apache.commons.collections4.CollectionUtils;
import com.zhb.broker.event.model.TimeWheelEvent;
import com.zhb.common.event.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description 所有时间轮的操作封装都尽量放在这里
 */
public class TimeWheelModelManager {

    private Logger logger = LoggerFactory.getLogger(TimeWheelModelManager.class);

    private final Object secondsLock = new Object();
    private final Object minutesLock = new Object();

    private long executeSeconds = 0l;

    private TimeWheelModel secondsTimeWheelModel;
    private TimeWheelModel minutesTimeWheelModel;
    private EventBus eventBus;


    /**
     * 初始化时间轮内部的变量值
     */
    public void init(EventBus eventBus) {
        secondsTimeWheelModel = new TimeWheelModel();
        secondsTimeWheelModel.setUnit(TimeWheelSlotStepUnitEnum.SECOND.getCode());
        secondsTimeWheelModel.setCurrent(0);
        secondsTimeWheelModel.setTimeWheelSlotListModel(buildTimeWheelSlotListModel(60));

        minutesTimeWheelModel = new TimeWheelModel();
        minutesTimeWheelModel.setCurrent(0);
        minutesTimeWheelModel.setTimeWheelSlotListModel(buildTimeWheelSlotListModel(60));
        minutesTimeWheelModel.setUnit(TimeWheelSlotStepUnitEnum.MINUTE.getCode());
        this.eventBus = eventBus;
        this.eventBus.init();
    }

    public void add(DelayMessageDTO delayMessageDTO) {
        int delay = delayMessageDTO.getDelay();
        // 2min 30s 执行 ，不推荐走层级时间轮的设计
        int min = delay / 60;
        if (min == 0) {
            synchronized (secondsLock) {
                int nextSlot = secondsTimeWheelModel.countNextSlot(delay);
                logger.info("current second slot:{},next slot:{}", secondsTimeWheelModel.getCurrent(), nextSlot);
                TimeWheelSlotListModel timeWheelSlotListModel = secondsTimeWheelModel.getTimeWheelSlotListModel()[nextSlot];
                TimeWheelSlotModel timeWheelSlotModel = new TimeWheelSlotModel();
                timeWheelSlotModel.setData(delayMessageDTO.getData());
                timeWheelSlotModel.setDelaySeconds(delayMessageDTO.getDelay());
                timeWheelSlotModel.setNextExecuteTime(delayMessageDTO.getNextExecuteTime());
                timeWheelSlotModel.setStoreType(delayMessageDTO.getSlotStoreType().getClazz());
                timeWheelSlotListModel.getTimeWheelSlotModels().add(timeWheelSlotModel);
            }
        } else if (min > 0) {
            synchronized (minutesLock) {
                int nextSlot = minutesTimeWheelModel.countNextSlot(min);
                logger.info("current minutes slot:{},next slot:{}", minutesTimeWheelModel.getCurrent(), nextSlot);
                TimeWheelSlotListModel timeWheelSlotListModel = minutesTimeWheelModel.getTimeWheelSlotListModel()[nextSlot];
                TimeWheelSlotModel timeWheelSlotModel = new TimeWheelSlotModel();
                timeWheelSlotModel.setData(delayMessageDTO.getData());
                timeWheelSlotModel.setDelaySeconds(delayMessageDTO.getDelay());
                timeWheelSlotModel.setNextExecuteTime(delayMessageDTO.getNextExecuteTime());
                timeWheelSlotModel.setStoreType(delayMessageDTO.getSlotStoreType().getClazz());
                timeWheelSlotListModel.getTimeWheelSlotModels().add(timeWheelSlotModel);
            }
        }
    }

    /**
     * 开启扫描slot数组任务
     */
    public void doScanTask() {
        Thread scanThread = new Thread(() -> {
            logger.info("start scan slot task");
            while (true) {
                try {
                    doSecondsTimeWheelExecute();
                    //正好一分钟整
                    if (executeSeconds % 60 == 0) {
                        doMinutesTimeWheelExecute();
                    }
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


    private void doSecondsTimeWheelExecute() {
        synchronized (secondsLock) {
//            logger.info("doScan second slot:{}", secondsTimeWheelModel.getCurrent());
            int current = secondsTimeWheelModel.getCurrent();
            TimeWheelSlotListModel timeWheelSlotListModel = secondsTimeWheelModel.getTimeWheelSlotListModel()[current];
            List<TimeWheelSlotModel> timeWheelSlotModelList = timeWheelSlotListModel.getTimeWheelSlotModels();
            if(CollectionUtils.isNotEmpty(timeWheelSlotModelList)) {
                TimeWheelEvent timeWheelEvent = new TimeWheelEvent();
                List<TimeWheelSlotModel> copySlotModel = timeWheelSlotModelList;
                timeWheelEvent.setTimeWheelSlotModelList(copySlotModel);
                eventBus.publish(timeWheelEvent);
            }
            //执行完成的任务，需要清空
            timeWheelSlotListModel.setTimeWheelSlotModels(new ArrayList<>());
            if (current == secondsTimeWheelModel.getTimeWheelSlotListModel().length - 1) {
                current = 0;
            } else {
                current = current + 1;
            }
            secondsTimeWheelModel.setCurrent(current);
//            logger.info("second slot's current is :{}",current);
        }
    }

    private void doMinutesTimeWheelExecute() {
        synchronized (minutesLock) {
            logger.info("doScan minutes slot:{}", minutesTimeWheelModel.getCurrent());
            int current = minutesTimeWheelModel.getCurrent();
            TimeWheelSlotListModel timeWheelSlotListModel = minutesTimeWheelModel.getTimeWheelSlotListModel()[current];
            List<TimeWheelSlotModel> timeWheelSlotModelList = timeWheelSlotListModel.getTimeWheelSlotModels();
            List<TimeWheelSlotModel> minutesTimeWheelModelList = new ArrayList<>();
            for (TimeWheelSlotModel timeWheelSlotModel : timeWheelSlotModelList) {
                int remainSecond = timeWheelSlotModel.getDelaySeconds() % 60;
                //如果延迟的时间不是分钟的整数（2min30s）
                if (remainSecond > 0) {
                    //需要被扔回到秒级别的时间轮
                    DelayMessageDTO delayMessageDTO = new DelayMessageDTO();
                    delayMessageDTO.setDelay(remainSecond);
                    delayMessageDTO.setSlotStoreType(SlotStoreTypeEnum.MESSAGE_RETRY_DTO);
                    delayMessageDTO.setData(timeWheelSlotModel.getData());
                    delayMessageDTO.setNextExecuteTime(timeWheelSlotModel.getNextExecuteTime());
                    add(delayMessageDTO);
                    logger.info("added message to second timeWheel");
                } else {
                    minutesTimeWheelModelList.add(timeWheelSlotModel);
                }
            }
            if (CollectionUtils.isNotEmpty(minutesTimeWheelModelList)) {
                //分钟整 直接执行业务逻辑
                TimeWheelEvent timeWheelEvent = new TimeWheelEvent();
                timeWheelEvent.setTimeWheelSlotModelList(timeWheelSlotModelList);
                eventBus.publish(timeWheelEvent);
            }
            //执行完成的任务，需要清空
            timeWheelSlotListModel.setTimeWheelSlotModels(new ArrayList<>());
            if (current == minutesTimeWheelModel.getTimeWheelSlotListModel().length - 1) {
                current = 0;
            } else {
                current = current + 1;
            }
            minutesTimeWheelModel.setCurrent(current);
        }
    }

    private TimeWheelSlotListModel[] buildTimeWheelSlotListModel(int count) {
        TimeWheelSlotListModel[] timeWheelSlotListModels = new TimeWheelSlotListModel[count];
        for (int i = 0; i < count; i++) {
            timeWheelSlotListModels[i] = new TimeWheelSlotListModel();
        }
        return timeWheelSlotListModels;
    }
}
