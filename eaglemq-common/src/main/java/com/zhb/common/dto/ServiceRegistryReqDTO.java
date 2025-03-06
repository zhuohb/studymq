package com.zhb.common.dto;

import com.zhb.common.enums.RegistryTypeEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author idea
 * @Date: Created in 20:07 2024/6/10
 * @Description 往nameserver进行服务注册时候的请求dto
 */
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

    public Map<String, Object> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    public String getRegistryType() {
        return registryType;
    }

    public void setRegistryType(String registryType) {
        this.registryType = registryType;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
