package com.zhb.nameserver.event.spi.listener;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.SlaveReplicationMsgAckEvent;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.SlaveAckDTO;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;

/**
 * @Author idea
 * @Date: Created in 13:48 2024/5/26
 * @Description 主节点接收从节点同步ack信号处理器
 */
public class SlaveReplicationMsgAckListener implements Listener<SlaveReplicationMsgAckEvent> {

	@Override
	public void onReceive(SlaveReplicationMsgAckEvent event) throws Exception {
		String slaveAckMsgId = event.getMsgId();
		SlaveAckDTO slaveAckDTO = CommonCache.getAckMap().get(slaveAckMsgId);
		if (slaveAckDTO == null) {
			return;
		}
		Integer currentAckTime = slaveAckDTO.getNeedAckTime().decrementAndGet();
		//如果是复制模式，代表所有从节点已经ack完毕了，
		//如果是半同步复制模式
		if (currentAckTime == 0) {
			CommonCache.getAckMap().remove(slaveAckMsgId);
			slaveAckDTO.getBrokerChannel().writeAndFlush(new TcpMsg(NameServerResponseCode.REGISTRY_SUCCESS.getCode(), NameServerResponseCode.REGISTRY_SUCCESS.getDesc().getBytes()));
		}
	}
}
