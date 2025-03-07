package com.zhb.broker.model;

import lombok.Getter;
import lombok.Setter;

/**
 * commitLog真实数据存储对象模型
 */
@Setter
@Getter
public class CommitLogMessageModel {

	/**
	 * 真正的消息内容
	 */
	private byte[] content;

	public byte[] convertToBytes() {
		return content;
	}
}
