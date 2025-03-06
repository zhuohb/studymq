package com.zhb.broker.model;

/**
 * @Author idea
 * @Date: Created at 2024/7/8
 * @Description master节点broker的地址信息
 */
public class MasterBrokerModel {

    private String brokerClusterGroup;
    private String brokerIp;
    private String brokerPort;

    public String getBrokerClusterGroup() {
        return brokerClusterGroup;
    }

    public void setBrokerClusterGroup(String brokerClusterGroup) {
        this.brokerClusterGroup = brokerClusterGroup;
    }

    public String getBrokerIp() {
        return brokerIp;
    }

    public void setBrokerIp(String brokerIp) {
        this.brokerIp = brokerIp;
    }

    public String getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(String brokerPort) {
        this.brokerPort = brokerPort;
    }
}
