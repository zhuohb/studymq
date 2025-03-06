package com.zhb.client.async.event.model;

import com.zhb.common.event.model.Event;

/**
 * @Author idea
 * @Date: Created at 2024/7/16
 * @Description
 */
public class BrokerConnectionClosedEvent extends Event {

    private String brokerReqId;

    public String getBrokerReqId() {
        return brokerReqId;
    }

    public void setBrokerReqId(String brokerReqId) {
        this.brokerReqId = brokerReqId;
    }
}
