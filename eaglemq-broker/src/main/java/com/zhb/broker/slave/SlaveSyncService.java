package com.zhb.broker.slave;

import com.alibaba.fastjson.JSON;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.StartSyncReqDTO;
import com.zhb.common.enums.BrokerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.remote.BrokerNettyRemoteClient;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * 从节点同步服务类
 * 负责Broker从节点与主节点之间的连接建立和数据同步
 * 在主从架构中，确保从节点能够从主节点获取最新的消息和配置
 */
@Slf4j
public class SlaveSyncService {

	/**
	 * Broker网络远程客户端
	 * 用于从节点与主节点之间的网络通信
	 */
	private BrokerNettyRemoteClient brokerNettyRemoteClient;

	/**
	 * 连接到主节点Broker服务器
	 * 解析地址字符串并建立网络连接，同时设置从节点同步处理器
	 *
	 * @param address 主节点地址，格式为"IP:PORT"
	 * @return 连接是否成功
	 */
	public boolean connectMasterBrokerNode(String address) {
		String addressAddr[] = address.split(":");
		String ip = addressAddr[0];
		Integer port = Integer.valueOf(addressAddr[1]);
		try {
			brokerNettyRemoteClient = new BrokerNettyRemoteClient(ip, port);
			brokerNettyRemoteClient.buildConnection(new SlaveSyncServerHandler(new EventBus("slave-sync-eventbus")));
			return true;
		} catch (Exception e) {
			log.error("error connect master broker", e);
		}
		return false;
	}

	/**
	 * 发送开始同步消息
	 * 向主节点发送请求，启动数据同步过程
	 * 同步请求包含唯一的消息ID用于跟踪请求状态
	 */
	public void sendStartSyncMsg() {
		StartSyncReqDTO startSyncReqDTO = new StartSyncReqDTO();
		startSyncReqDTO.setMsgId(UUID.randomUUID().toString());
		TcpMsg tcpMsg = new TcpMsg(BrokerEventCode.START_SYNC_MSG.getCode(), JSON.toJSONBytes(startSyncReqDTO));
		TcpMsg startSyncMsgResp = brokerNettyRemoteClient.sendSyncMsg(tcpMsg, startSyncReqDTO.getMsgId());
		log.info("startSyncMsgResp is:{}", JSON.toJSONString(startSyncMsgResp));
	}
}
