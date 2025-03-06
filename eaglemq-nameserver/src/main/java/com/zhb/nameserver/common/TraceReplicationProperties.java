package com.zhb.nameserver.common;

/**
 * @Author idea
 * @Date: Created in 09:07 2024/5/15
 * @Description 链路化方式同步配置
 */
public class TraceReplicationProperties {

	private String nextNode;

	private Integer port;

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getNextNode() {
		return nextNode;
	}

	public void setNextNode(String nextNode) {
		this.nextNode = nextNode;
	}
}
