package com.zhb.common.dto;

import com.zhb.common.enums.LocalTransactionState;
import com.zhb.common.enums.MessageSendWay;
import com.zhb.common.enums.TxMessageFlagEnum;

/**
 * mq消息发送参数
 */
public class MessageDTO {

	private String topic;
	private int queueId = -1;
	private String msgId;
	/**
	 * 发送方式（同步/异步）
	 *
	 * @see MessageSendWay
	 */
	private int sendWay;
	private byte[] body;
	private boolean isRetry;
	private int currentRetryTimes;
	//延迟的时间 秒单位
	private int delay;
	private String producerId;
	/**
	 * @see TxMessageFlagEnum
	 */
	private int txFlag = -1;
	/**
	 * @see LocalTransactionState
	 */
	private int localTxState = -1;

	public String getProducerId() {
		return producerId;
	}

	public void setProducerId(String producerId) {
		this.producerId = producerId;
	}

	public int getLocalTxState() {
		return localTxState;
	}

	public void setLocalTxState(int localTxState) {
		this.localTxState = localTxState;
	}

	public int getTxFlag() {
		return txFlag;
	}

	public void setTxFlag(int txFlag) {
		this.txFlag = txFlag;
	}

	public int getCurrentRetryTimes() {
		return currentRetryTimes;
	}

	public void setCurrentRetryTimes(int currentRetryTimes) {
		this.currentRetryTimes = currentRetryTimes;
	}

	public boolean isRetry() {
		return isRetry;
	}

	public void setRetry(boolean retry) {
		isRetry = retry;
	}

	public int getSendWay() {
		return sendWay;
	}

	public void setSendWay(int sendWay) {
		this.sendWay = sendWay;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}
}
