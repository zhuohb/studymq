package com.zhb.nameserver.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Setter
@Getter
@Slf4j
public class NameserverProperties {


	private String nameserverUser;
	private String nameserverPwd;
	private Integer nameserverPort;
	private String replicationMode;
	private TraceReplicationProperties traceReplicationProperties;
	private MasterSlaveReplicationProperties masterSlaveReplicationProperties;

	public void print() {
		log.info(JSON.toJSONString(this, SerializerFeature.PrettyFormat));
	}
}
