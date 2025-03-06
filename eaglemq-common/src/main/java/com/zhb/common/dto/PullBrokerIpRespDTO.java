package com.zhb.common.dto;

import java.util.List;

/**
 * @Author idea
 * @Date: Created in 16:52 2024/6/11
 * @Description
 */
public class PullBrokerIpRespDTO extends BaseNameServerRemoteDTO{

    private List<String> addressList;

    private List<String> masterAddressList;

    private List<String> slaveAddressList;

    public List<String> getSlaveAddressList() {
        return slaveAddressList;
    }

    public void setSlaveAddressList(List<String> slaveAddressList) {
        this.slaveAddressList = slaveAddressList;
    }

    public List<String> getMasterAddressList() {
        return masterAddressList;
    }

    public void setMasterAddressList(List<String> masterAddressList) {
        this.masterAddressList = masterAddressList;
    }

    public List<String> getAddressList() {
        return addressList;
    }

    public void setAddressList(List<String> addressList) {
        this.addressList = addressList;
    }
}
