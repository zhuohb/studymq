package com.zhb.common.dto;

import com.zhb.common.enums.RegistryTypeEnum;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * 往nameserver进行服务注册时候的请求dto
 */
@Setter
@Getter
public class ServiceRegistryReqDTO extends BaseNameServerRemoteDTO{

    /**
     * 节点的注册类型，方便统计数据使用
     * @see RegistryTypeEnum
     */
    private String registryType;
    private String user;
    private String password;
    private String ip;
    private Integer port;
    //后续会用到 todo
    private Map<String,Object> attrs = new HashMap<>();

}
