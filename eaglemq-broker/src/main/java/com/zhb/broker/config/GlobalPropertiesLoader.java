package com.zhb.broker.config;

import io.netty.util.internal.StringUtil;
import com.zhb.broker.cache.CommonCache;
import com.zhb.common.constants.BrokerConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @Author idea
 * @Date: Created in 08:50 2024/3/26
 * @Description
 */
public class GlobalPropertiesLoader {

    public void loadProperties() {
        GlobalProperties globalProperties = new GlobalProperties();
        String eagleMqHome = System.getenv(BrokerConstants.EAGLE_MQ_HOME);
        if (StringUtil.isNullOrEmpty(eagleMqHome)) {
            throw new IllegalArgumentException("EAGLE_MQ_HOME is null");
        }
        globalProperties.setEagleMqHome(eagleMqHome);
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(eagleMqHome+BrokerConstants.BROKER_PROPERTIES_PATH)));
            globalProperties.setNameserverIp(properties.getProperty("nameserver.ip"));
            globalProperties.setNameserverPort(Integer.valueOf(properties.getProperty("nameserver.port")));
            globalProperties.setNameserverUser(properties.getProperty("nameserver.user"));
            globalProperties.setBrokerPort(Integer.valueOf(properties.getProperty("broker.port")));
            globalProperties.setNameserverPassword(properties.getProperty("nameserver.password"));
            globalProperties.setReBalanceStrategy(properties.getProperty("rebalance.strategy"));

            globalProperties.setBrokerClusterGroup(properties.getProperty("broker.cluster.group"));
            globalProperties.setBrokerClusterMode(properties.getProperty("broker.cluster.mode"));
            globalProperties.setBrokerClusterRole(properties.getProperty("broker.cluster.role"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        CommonCache.setGlobalProperties(globalProperties);
    }
}
