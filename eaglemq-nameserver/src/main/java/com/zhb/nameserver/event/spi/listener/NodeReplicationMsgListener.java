package com.zhb.nameserver.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.common.TraceReplicationProperties;
import com.zhb.nameserver.event.model.NodeReplicationAckMsgEvent;
import com.zhb.nameserver.event.model.NodeReplicationMsgEvent;
import com.zhb.nameserver.event.model.ReplicationMsgEvent;
import com.zhb.nameserver.store.ServiceInstance;
import io.netty.util.internal.StringUtil;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.event.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;

/**
 * @Author idea
 * @Date: Created in 10:19 2024/6/1
 * @Description 接收上一个节点同步过来的复制数据内容
 */
public class NodeReplicationMsgListener implements Listener<NodeReplicationMsgEvent> {

	private final Logger logger = LoggerFactory.getLogger(NodeReplicationMsgListener.class);


	@Override
	public void onReceive(NodeReplicationMsgEvent event) throws Exception {
		ServiceInstance serviceInstance = event.getServiceInstance();
		//接收到上一个节点同步过来的数据，然后存入本地内存
		CommonCache.getServiceInstanceManager().put(serviceInstance);
		ReplicationMsgEvent replicationMsgEvent = new ReplicationMsgEvent();
		replicationMsgEvent.setServiceInstance(serviceInstance);
		replicationMsgEvent.setMsgId(event.getMsgId());
		replicationMsgEvent.setType(event.getType());
		logger.info("接收到上一个节点写入的数据:{}", JSON.toJSONString(replicationMsgEvent));
		CommonCache.getReplicationMsgQueueManager().put(replicationMsgEvent);
		TraceReplicationProperties traceReplicationProperties = CommonCache.getNameserverProperties().getTraceReplicationProperties();
		if (StringUtil.isNullOrEmpty(traceReplicationProperties.getNextNode())) {
			//如果是尾部节点，不需要再给下一个节点做复制，但是要返回ack给上一个节点
			//node1->node2->node3->node4
			logger.info("当前是尾部节点，返回ack给上一个节点");
			NodeReplicationAckMsgEvent nodeReplicationAckMsgEvent = new NodeReplicationAckMsgEvent();
			nodeReplicationAckMsgEvent.setNodeIp(Inet4Address.getLocalHost().getHostAddress());
			nodeReplicationAckMsgEvent.setType(replicationMsgEvent.getType());
			nodeReplicationAckMsgEvent.setNodePort(traceReplicationProperties.getPort());
			nodeReplicationAckMsgEvent.setMsgId(replicationMsgEvent.getMsgId());
			CommonCache.getPreNodeChannel().writeAndFlush(new TcpMsg(NameServerEventCode.NODE_REPLICATION_ACK_MSG.getCode(), JSON.toJSONBytes(nodeReplicationAckMsgEvent)));
		}
	}
}
