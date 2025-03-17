package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;


@Setter
@Getter
public class PullBrokerIpRespDTO extends BaseNameServerRemoteDTO{

    private List<String> addressList;

    private List<String> masterAddressList;

    private List<String> slaveAddressList;

}
