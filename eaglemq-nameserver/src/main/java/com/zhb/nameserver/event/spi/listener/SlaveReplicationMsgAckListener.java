package com.zhb.nameserver.event.spi.listener;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.SlaveReplicationMsgAckEvent;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.SlaveAckDTO;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;

/**
 * 从节点复制消息确认监听器
 * <p>
 * 负责处理主节点接收到的从节点同步确认(ACK)信号，实现主从架构中的数据同步确认机制
 * 是主从复制模式下确保数据一致性的关键组件，通过ACK计数确定同步状态
 * <p>
 * 主要功能：
 * 1. 接收从节点发送的数据同步确认信号
 * 2. 维护和跟踪每个消息的确认状态
 * 3. 根据确认计数判断是否所有必要的从节点都已确认接收
 * 4. 当所有必要确认完成时，向原始请求方发送成功响应
 * <p>
 * 使用场景：
 * - 主从复制模式下，确保服务注册等关键操作在足够数量的从节点上完成同步
 * - 支持半同步复制模式，在达到指定数量的从节点确认后即返回成功
 * - 提供数据一致性保证，确保集群中数据的可靠性
 */
public class SlaveReplicationMsgAckListener implements Listener<SlaveReplicationMsgAckEvent> {

	/**
	 * 处理从节点的同步确认事件
	 * <p>
	 * 接收并处理从节点发送的数据同步确认信号，跟踪每个消息的确认状态，
	 * 当所有必要的确认都收到后，发送成功响应给原始请求方。
	 * <p>
	 * 处理流程：
	 * 1. 根据消息ID获取对应的确认追踪对象
	 * 2. 减少需要确认的计数器
	 * 3. 判断是否所有必要的确认都已收到
	 * 4. 如果确认完成，从跟踪映射中移除该消息并发送成功响应
	 *
	 * @param event 从节点发送的确认事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(SlaveReplicationMsgAckEvent event) throws Exception {
		// 获取从节点确认的消息ID
		String slaveAckMsgId = event.getMsgId();
		// 从确认映射表中获取对应的确认追踪对象
		SlaveAckDTO slaveAckDTO = CommonCache.getAckMap().get(slaveAckMsgId);
		if (slaveAckDTO == null) {
			// 如果找不到对应的确认对象，可能是超时已被清理或重复确认，直接返回
			return;
		}

		// 减少需要确认的计数，并获取当前剩余需要确认的数量
		Integer currentAckTime = slaveAckDTO.getNeedAckTime().decrementAndGet();

		// 当确认计数为0时，表示所有必要的从节点都已确认接收
		// 适用于全复制模式(所有从节点确认)或半同步复制模式(部分从节点确认)
		if (currentAckTime == 0) {
			// 从确认映射表中移除该消息的追踪对象，释放资源
			CommonCache.getAckMap().remove(slaveAckMsgId);
			// 向原始请求的broker发送注册成功响应，完成整个注册流程
			slaveAckDTO.getBrokerChannel().writeAndFlush(new TcpMsg(NameServerResponseCode.REGISTRY_SUCCESS.getCode(), NameServerResponseCode.REGISTRY_SUCCESS.getDesc().getBytes()));
		}
	}
}
