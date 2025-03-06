package com.zhb.broker.core;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.common.cache.BrokerServerSyncFutureManager;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.constants.BrokerConstants;
import com.zhb.common.dto.MessageDTO;
import com.zhb.common.dto.SendMessageToBrokerResponseDTO;
import com.zhb.common.dto.SlaveSyncRespDTO;
import com.zhb.common.enums.*;
import com.zhb.common.event.model.Event;
import com.zhb.common.remote.SyncFuture;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Slf4j
public class CommitLogAppendHandler {
	public void prepareMMapLoading(String topicName) throws IOException {
		CommitLogMMapFileModel mapFileModel = new CommitLogMMapFileModel();
		mapFileModel.loadFileInMMap(topicName, 0, BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE);
		CommonCache.getCommitLogMMapFileModelManager().put(topicName, mapFileModel);
	}

	public void appendMsg(MessageDTO messageDTO, Event event) throws IOException {
		CommonCache.getCommitLogAppendHandler().appendMsg(messageDTO);
		int sendWay = messageDTO.getSendWay();
		boolean isAsyncSend = MessageSendWay.ASYNC.getCode() == sendWay;
		//判断下是主节点还是从节点
		boolean isClusterMode = BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(CommonCache.getGlobalProperties().getBrokerClusterMode());
		boolean isMasterNode = "master".equals(CommonCache.getGlobalProperties().getBrokerClusterRole());
		boolean isDelayMsg = messageDTO.getDelay() > 0;
		if (isClusterMode) {
			if (isMasterNode) {
				//主节点 发送同步请求给从节点,异步发送是没有消息id的
				for (ChannelHandlerContext slaveChannel : CommonCache.getSlaveChannelMap().values()) {
					slaveChannel.writeAndFlush(new TcpMsg(BrokerEventCode.PUSH_MSG.getCode(), JSON.toJSONBytes(messageDTO)));
				}
				if (isAsyncSend || isDelayMsg) {
					return;
				}
				//主从一开始是正常的，但是后边从节点断开了
				if (CommonCache.getSlaveChannelMap().isEmpty()) {
					//可能此时从节点全部中断了，所以没法同步,可以直接返回成功给到客户端，保证整体可用
					SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
					sendMsgResp.setMsgId(messageDTO.getMsgId());
					sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
					sendMsgResp.setDesc("send msg success,but current time has no slave node!");
					TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.SEND_MSG_RESP.getCode(), JSON.toJSONBytes(sendMsgResp));
					event.getChannelHandlerContext().writeAndFlush(responseMsg);
					return;
				}
				SyncFuture syncFuture = new SyncFuture();
				syncFuture.setMsgId(messageDTO.getMsgId());
				BrokerServerSyncFutureManager.put(messageDTO.getMsgId(), syncFuture);
				SyncFuture slaveSyncAckRespFuture = BrokerServerSyncFutureManager.get(messageDTO.getMsgId());
				if (slaveSyncAckRespFuture != null) {
					SlaveSyncRespDTO slaveSyncRespDTO = null;
					SendMessageToBrokerResponseDTO sendMsgResp = new SendMessageToBrokerResponseDTO();
					sendMsgResp.setMsgId(messageDTO.getMsgId());
					sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.FAIL.getCode());
					try {
						//主从网络延迟非常严重
						slaveSyncRespDTO = (SlaveSyncRespDTO) slaveSyncAckRespFuture.get(3, TimeUnit.SECONDS);
						if (slaveSyncRespDTO.isSyncSuccess()) {
							sendMsgResp.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
						}
						//超时等同步一系列问题全部注入到响应体中返回给到客户端
					} catch (InterruptedException e) {
						sendMsgResp.setDesc("Slave node sync fail! Sync task had InterruptedException!");
						log.error("slave sync error is:", e);
					} catch (ExecutionException e) {
						sendMsgResp.setDesc("Slave node sync fail! Sync task had ExecutionException");
						log.error("slave sync error is:", e);
					} catch (TimeoutException e) {
						sendMsgResp.setDesc("Slave node sync fail! Sync task had TimeoutException");
						log.error("slave sync error is:", e);
					} catch (Exception e) {
						sendMsgResp.setDesc("Slave node sync unKnow error! Sync task had Exception");
						log.error("slave sync unKnow error is:", e);
					}
					//响应返回给到客户端，完成主从复制链路效果
					TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.SEND_MSG_RESP.getCode(), JSON.toJSONBytes(sendMsgResp));
					event.getChannelHandlerContext().writeAndFlush(responseMsg);
				}
			} else {
				if (isAsyncSend || isDelayMsg) {
					return;
				}
				//从节点 返回响应code给主节点
				SlaveSyncRespDTO slaveSyncAckRespDTO = new SlaveSyncRespDTO();
				slaveSyncAckRespDTO.setSyncSuccess(true);
				slaveSyncAckRespDTO.setMsgId(messageDTO.getMsgId());
				event.getChannelHandlerContext().writeAndFlush(new TcpMsg(BrokerResponseCode.SLAVE_SYNC_RESP.getCode(),
					JSON.toJSONBytes(slaveSyncAckRespDTO)));
				return;
			}
		} else {
			//单机版本处理逻辑
			if (isAsyncSend || isDelayMsg) {
				return;
			}
			SendMessageToBrokerResponseDTO sendMessageToBrokerResponseDTO = new SendMessageToBrokerResponseDTO();
			sendMessageToBrokerResponseDTO.setStatus(SendMessageToBrokerResponseStatus.SUCCESS.getCode());
			sendMessageToBrokerResponseDTO.setMsgId(messageDTO.getMsgId());
			TcpMsg responseMsg = new TcpMsg(BrokerResponseCode.SEND_MSG_RESP.getCode(), JSON.toJSONBytes(sendMessageToBrokerResponseDTO));
			event.getChannelHandlerContext().writeAndFlush(responseMsg);
		}
	}

	public void appendMsg(MessageDTO messageDTO) throws IOException {
		CommitLogMMapFileModel mapFileModel = CommonCache.getCommitLogMMapFileModelManager().get(messageDTO.getTopic());
		if (mapFileModel == null) {
			throw new RuntimeException("topic is invalid!");
		}
		mapFileModel.writeContent(messageDTO, true);
	}

}
