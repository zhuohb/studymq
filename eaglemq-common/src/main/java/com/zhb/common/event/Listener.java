package com.zhb.common.event;

import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created in 14:24 2024/5/4
 * @Description
 */
public interface Listener<E extends Event> {

    /**
     * 回调通知
     *
     * @param event
     */
    void onReceive(E event) throws Exception;
}
