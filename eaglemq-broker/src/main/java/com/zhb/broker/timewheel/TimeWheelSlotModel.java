package com.zhb.broker.timewheel;


/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description
 */
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
     * @see SlotStoreTypeEnum
     */
    private Class storeType;

    /**
     * 下一次执行时间
     */
    private long nextExecuteTime;

    public long getNextExecuteTime() {
        return nextExecuteTime;
    }

    public void setNextExecuteTime(long nextExecuteTime) {
        this.nextExecuteTime = nextExecuteTime;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Class getStoreType() {
        return storeType;
    }

    public void setStoreType(Class storeType) {
        this.storeType = storeType;
    }

    public int getDelaySeconds() {
        return delaySeconds;
    }

    public void setDelaySeconds(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }
}
