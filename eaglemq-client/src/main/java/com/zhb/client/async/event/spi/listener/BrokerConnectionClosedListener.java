package com.zhb.client.async.event.spi.listener;

import com.zhb.client.async.event.model.BrokerConnectionClosedEvent;
import com.zhb.common.event.Listener;


/**
 * @Author idea
 * @Date: Created at 2024/7/16
 * @Description broker链接断开监听器
 */
public class BrokerConnectionClosedListener implements Listener<BrokerConnectionClosedEvent> {

    @Override
    public void onReceive(BrokerConnectionClosedEvent event) throws Exception {
        String brokerReqId = event.getBrokerReqId();
        System.out.println("channel断开：" + brokerReqId);
    }
}
