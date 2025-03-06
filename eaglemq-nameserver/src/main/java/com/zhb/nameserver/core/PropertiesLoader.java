package com.zhb.nameserver.core;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.MasterSlaveReplicationProperties;
import com.zhb.nameserver.common.NameserverProperties;
import com.zhb.nameserver.common.TraceReplicationProperties;
import com.zhb.common.constants.BrokerConstants;
import com.zhb.nameserver.common.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @Author idea
 * @Date: Created in 17:25 2024/5/4
 * @Description
 */
public class PropertiesLoader {

	private Properties properties = new Properties();

	public void loadProperties() throws IOException {
		String eagleMqHome = System.getenv(BrokerConstants.EAGLE_MQ_HOME);
		properties.load(new FileInputStream(new File(eagleMqHome + "/config/nameserver.properties")));
		NameserverProperties nameserverProperties = new NameserverProperties();
		nameserverProperties.setNameserverPwd(getStr("nameserver.password"));
		nameserverProperties.setNameserverUser(getStr("nameserver.user"));
		nameserverProperties.setNameserverPort(getInt("nameserver.port"));
		nameserverProperties.setReplicationMode(getStrCanBeNull("nameserver.replication.mode"));
		TraceReplicationProperties traceReplicationProperties = new TraceReplicationProperties();
		traceReplicationProperties.setNextNode(getStrCanBeNull("nameserver.replication.trace.next.node"));
		traceReplicationProperties.setPort(getIntCanBeNull("nameserver.replication.trace.port"));
		nameserverProperties.setTraceReplicationProperties(traceReplicationProperties);
		MasterSlaveReplicationProperties masterSlaveReplicationProperties = new MasterSlaveReplicationProperties();
		masterSlaveReplicationProperties.setMaster(getStrCanBeNull("nameserver.replication.master"));
		masterSlaveReplicationProperties.setRole(getStrCanBeNull("nameserver.replication.master.slave.role"));
		masterSlaveReplicationProperties.setType(getStrCanBeNull("nameserver.replication.master.slave.type"));
		masterSlaveReplicationProperties.setPort(getInt("nameserver.replication.port"));
		nameserverProperties.setMasterSlaveReplicationProperties(masterSlaveReplicationProperties);
		nameserverProperties.print();
		CommonCache.setNameserverProperties(nameserverProperties);
	}

	private String getStrCanBeNull(String key) {
		String value = properties.getProperty(key);
		if (value == null) {
			return "";
		}
		return value;
	}

	private String getStr(String key) {
		String value = properties.getProperty(key);
		if (value == null) {
			throw new RuntimeException("配置参数：" + key + "不存在");
		}
		return value;
	}

	private Integer getIntCanBeNull(String key) {
		String intValue = getStr(key);
		if (intValue == null) {
			return null;
		}
		return Integer.valueOf(intValue);
	}

	private Integer getInt(String key) {
		return Integer.valueOf(getStr(key));
	}

	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
