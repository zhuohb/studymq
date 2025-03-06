package com.zhb.nameserver.enums;

/**
 * @Author idea
 * @Date: Created in 16:11 2024/5/18
 * @Description
 */
public enum ReplicationRoleEnum {

	MASTER("master", "主从-主"),
	SLAVE("slave", "主从-从"),
	NODE("not_tail_node", "链路复制-非尾部节点"),
	TAIL_NODE("tail_node", "链路复制-尾部节点"),
	;
	String code;
	String desc;

	ReplicationRoleEnum(String code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	public static ReplicationRoleEnum of(String code) {
		for (ReplicationRoleEnum value : values()) {
			if (value.getCode().equals(code)) {
				return value;
			}
		}
		return null;
	}

	public String getCode() {
		return code;
	}

	public String getDesc() {
		return desc;
	}
}
