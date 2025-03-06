package com.zhb.nameserver.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author idea
 * @Date: Created in 09:04 2024/5/15
 * @Description
 */
public class NameserverProperties {

	private final Logger logger = LoggerFactory.getLogger(NameserverProperties.class);

	private String nameserverUser;
	private String nameserverPwd;
	private Integer nameserverPort;
	private String replicationMode;
	private TraceReplicationProperties traceReplicationProperties;
	private MasterSlaveReplicationProperties masterSlaveReplicationProperties;

	public String getNameserverUser() {
		return nameserverUser;
	}

	public void setNameserverUser(String nameserverUser) {
		this.nameserverUser = nameserverUser;
	}

	public Integer getNameserverPort() {
		return nameserverPort;
	}

	public void setNameserverPort(Integer nameserverPort) {
		this.nameserverPort = nameserverPort;
	}

	public String getReplicationMode() {
		return replicationMode;
	}

	public void setReplicationMode(String replicationMode) {
		this.replicationMode = replicationMode;
	}

	public String getNameserverPwd() {
		return nameserverPwd;
	}

	public void setNameserverPwd(String nameserverPwd) {
		this.nameserverPwd = nameserverPwd;
	}

	public TraceReplicationProperties getTraceReplicationProperties() {
		return traceReplicationProperties;
	}

	public void setTraceReplicationProperties(TraceReplicationProperties traceReplicationProperties) {
		this.traceReplicationProperties = traceReplicationProperties;
	}

	public MasterSlaveReplicationProperties getMasterSlaveReplicationProperties() {
		return masterSlaveReplicationProperties;
	}

	public void setMasterSlaveReplicationProperties(MasterSlaveReplicationProperties masterSlaveReplicationProperties) {
		this.masterSlaveReplicationProperties = masterSlaveReplicationProperties;
	}

	public void print() {
		logger.info(JSON.toJSONString(this, SerializerFeature.PrettyFormat));
	}
}
