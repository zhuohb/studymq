package com.zhb.nameserver.enums;

/**
 * @Author idea
 * @Date: Created in 13:23 2024/5/26
 * @Description 主从同步的复制模式
 */
public enum MasterSlaveReplicationTypeEnum {

	SYNC("sync", "同步复制"),  //master -> slave * 2,客户端写入数据往master写入，数据发送给到下游的slave节点，slave节点确认数据存储好之后，返回ack，需要有所有从节点返回ack，master再告诉客户端写入成功
	HALF_SYNC("half_sync", "半同步复制"),  //master -> slave * 5,客户端写入数据往master写入，数据发送给到下游的slave节点，slave节点确认数据存储好之后，返回ack，如果有超过半数节点返回ack，则master再告诉客户端写入成功
	ASYNC("async", "异步复制"); //master -> slave * 2,客户端写入数据往master写入，数据写入到master的存储后，master的一个异步线程发送同步数据给到slave，快速通知客户端写入成功
	String code;
	String desc;

	MasterSlaveReplicationTypeEnum(String code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	public String getCode() {
		return code;
	}

	public String getDesc() {
		return desc;
	}

	public static MasterSlaveReplicationTypeEnum of(String code) {
		for (MasterSlaveReplicationTypeEnum value : MasterSlaveReplicationTypeEnum.values()) {
			if (value.getCode().equals(code)) {
				return value;
			}
		}
		return null;
	}
}
