package com.zhb.broker.model;

import com.zhb.broker.utils.ByteConvertUtils;
import lombok.Getter;
import lombok.Setter;

/**
 * consumerQueue数据结构存储的最小单元对象
 */
@Setter
@Getter
public class ConsumeQueueDetailModel {

	private int commitLogFilename;
	//4byte
	private int msgIndex; //commitlog数据存储的地址，mmap映射的地址，Integer.MAX校验

	private int msgLength;

	private int retryTimes;


	public byte[] convertToBytes() {
		byte[] commitLogFileNameBytes = ByteConvertUtils.intToBytes(commitLogFilename);
		byte[] msgIndexBytes = ByteConvertUtils.intToBytes(msgIndex);
		byte[] msgLengthBytes = ByteConvertUtils.intToBytes(msgLength);
		byte[] retryTimeBytes = ByteConvertUtils.intToBytes(retryTimes);
		byte[] finalBytes = new byte[16];
		int p = 0;
		for (int i = 0; i < 4; i++) {
			finalBytes[p++] = commitLogFileNameBytes[i];
		}
		for (int i = 0; i < 4; i++) {
			finalBytes[p++] = msgIndexBytes[i];
		}
		for (int i = 0; i < 4; i++) {
			finalBytes[p++] = msgLengthBytes[i];
		}
		for (int i = 0; i < 4; i++) {
			finalBytes[p++] = retryTimeBytes[i];
		}
		return finalBytes;
	}

	public void buildFromBytes(byte[] body) {
		this.setCommitLogFilename(ByteConvertUtils.bytesToInt(ByteConvertUtils.readInPos(body, 0, 4)));
		this.setMsgIndex(ByteConvertUtils.bytesToInt(ByteConvertUtils.readInPos(body, 4, 4)));
		this.setMsgLength(ByteConvertUtils.bytesToInt(ByteConvertUtils.readInPos(body, 8, 4)));
		this.setRetryTimes(ByteConvertUtils.bytesToInt(ByteConvertUtils.readInPos(body, 12, 4)));
	}


}
