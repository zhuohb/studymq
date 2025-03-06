package com.zhb.broker.timewheel;

/**
 * @Author idea
 * @Date: Created at 2024/7/28
 * @Description
 */
public class DelayMessageDTO {

    /**
     * 原始数据
     */
    private Object data;

    /**
     * @see SlotStoreTypeEnum
     */
    private SlotStoreTypeEnum slotStoreType;

    /**
     * 延迟多久 秒
     */
    private int delay;

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


    public SlotStoreTypeEnum getSlotStoreType() {
        return slotStoreType;
    }

    public void setSlotStoreType(SlotStoreTypeEnum slotStoreType) {
        this.slotStoreType = slotStoreType;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }
}
