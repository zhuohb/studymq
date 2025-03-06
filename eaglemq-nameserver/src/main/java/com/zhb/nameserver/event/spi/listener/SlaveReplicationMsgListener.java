package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.event.model.SlaveReplicationMsgAckEvent;
import com.zhb.nameserver.store.ServiceInstance;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.Listener;

/**
 * @Author idea
 * @Date: Created in 23:17 2024/5/21
 * @Description 从节点专属的数据同步监听器
 */
public class SlaveReplicationMsgListener implements Listener<ReplicationMsgEvent> {


	@Override
	public void onReceive(ReplicationMsgEvent event) throws Exception {
		ServiceInstance serviceInstance = event.getServiceInstance();
		//从节点接收主节点同步数据逻辑
		CommonCache.getServiceInstanceManager().put(serviceInstance);
		SlaveReplicationMsgAckEvent slaveReplicationMsgAckEvent = new SlaveReplicationMsgAckEvent();
		slaveReplicationMsgAckEvent.setMsgId(event.getMsgId());
		event.getChannelHandlerContext().channel().writeAndFlush(new TcpMsg(NameServerEventCode.SLAVE_REPLICATION_ACK_MSG.ordinal(),
			JSON.toJSONBytes(slaveReplicationMsgAckEvent)));
	}
}
