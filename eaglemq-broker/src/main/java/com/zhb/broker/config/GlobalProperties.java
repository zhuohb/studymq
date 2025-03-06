package com.zhb.broker.config;

/**
 * @Author idea
 * @Date: Created in 08:49 2024/3/26
 * @Description
 */
public class GlobalProperties {

    //nameserver的属性
    private String nameserverIp;
    private Integer nameserverPort;
    private String nameserverUser;
    private String nameserverPassword;
    private Integer brokerPort;
    //重平衡策略
    private String reBalanceStrategy;

    //集群配置相关属性
    private String brokerClusterMode;
    private String brokerClusterRole;
    private String brokerClusterGroup;

    /**
     * 读取环境变量中配置的mq存储绝对路径地址
     */
    private String eagleMqHome;

    public String getEagleMqHome() {
        return eagleMqHome;
    }

    public void setEagleMqHome(String eagleMqHome) {
        this.eagleMqHome = eagleMqHome;
    }

    public String getNameserverIp() {
        return nameserverIp;
    }

    public void setNameserverIp(String nameserverIp) {
        this.nameserverIp = nameserverIp;
    }

    public Integer getNameserverPort() {
        return nameserverPort;
    }

    public void setNameserverPort(Integer nameserverPort) {
        this.nameserverPort = nameserverPort;
    }

    public String getNameserverUser() {
        return nameserverUser;
    }

    public void setNameserverUser(String nameserverUser) {
        this.nameserverUser = nameserverUser;
    }

    public String getNameserverPassword() {
        return nameserverPassword;
    }

    public void setNameserverPassword(String nameserverPassword) {
        this.nameserverPassword = nameserverPassword;
    }

    public Integer getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(Integer brokerPort) {
        this.brokerPort = brokerPort;
    }

    public String getReBalanceStrategy() {
        return reBalanceStrategy;
    }

    public void setReBalanceStrategy(String reBalanceStrategy) {
        this.reBalanceStrategy = reBalanceStrategy;
    }

    public String getBrokerClusterMode() {
        return brokerClusterMode;
    }

    public void setBrokerClusterMode(String brokerClusterMode) {
        this.brokerClusterMode = brokerClusterMode;
    }

    public String getBrokerClusterRole() {
        return brokerClusterRole;
    }

    public void setBrokerClusterRole(String brokerClusterRole) {
        this.brokerClusterRole = brokerClusterRole;
    }

    public String getBrokerClusterGroup() {
        return brokerClusterGroup;
    }

    public void setBrokerClusterGroup(String brokerClusterGroup) {
        this.brokerClusterGroup = brokerClusterGroup;
    }
}
