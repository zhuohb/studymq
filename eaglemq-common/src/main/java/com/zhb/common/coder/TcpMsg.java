package com.zhb.common.coder;

import com.zhb.common.constants.BrokerConstants;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class TcpMsg {
	//魔数
	private short magic;
	//表示请求包的具体含义
	private int code;
	//消息长度
	private int len;
	private byte[] body;

	public TcpMsg(int code, byte[] body) {
		this.magic = BrokerConstants.DEFAULT_MAGIC_NUM;
		this.code = code;
		this.body = body;
		this.len = body.length;
	}

}
