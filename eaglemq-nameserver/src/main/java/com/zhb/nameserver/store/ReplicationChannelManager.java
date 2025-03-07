package com.zhb.nameserver.store;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 复制通道管理器
 * <p>
 * 该类负责管理NameServer集群中节点间的复制通信通道，是实现数据复制和同步的核心组件。
 * 主要管理主节点与从节点之间的通信连接，支持主从复制架构中的消息传递和心跳检测。
 * <p>
 * 功能特点：
 * 1. 使用线程安全的ConcurrentHashMap存储通道映射关系，确保在高并发环境下的一致性
 * 2. 提供通道的增加、获取和有效性检测能力，支持动态扩缩容
 * 3. 自动过滤失效的通道连接，维护集群的健康状态
 * <p>
 * 应用场景：
 * - 主节点向多个从节点发送复制消息
 * - 主节点检测从节点在线状态
 * - 复制架构中节点间的通信管理
 */
public class ReplicationChannelManager {

	/**
	 * 复制通道映射表，存储请求ID到对应通道上下文的映射关系
	 * <p>
	 * 采用ConcurrentHashMap确保在多线程环境下的安全访问，
	 * key为请求ID（通常是从节点标识），value为对应的通道处理上下文
	 */
	private static Map<String, ChannelHandlerContext> channelHandlerContextMap = new ConcurrentHashMap<>();

	/**
	 * 获取完整的通道映射表
	 * <p>
	 * 返回所有已注册的通道映射，包括可能已失效的通道。
	 * 如果需要仅获取有效通道，应使用{@link #getValidSlaveChannelMap()}方法。
	 *
	 * @return 包含所有已注册通道的映射表
	 */
	public Map<String, ChannelHandlerContext> getChannelHandlerContextMap() {
		return channelHandlerContextMap;
	}

	/**
	 * 获取所有有效的从节点通道映射
	 * <p>
	 * 该方法会遍历所有注册的通道，检测其连接状态，
	 * 并过滤掉已失效的通道连接，确保返回的映射表中只包含活跃可用的通道。
	 * <p>
	 * 处理流程：
	 * 1. 遍历当前所有通道，检查其活跃状态
	 * 2. 将不活跃的通道标识添加到待移除列表
	 * 3. 从主映射表中移除所有不活跃的通道
	 * 4. 返回清理后的有效通道映射表
	 *
	 * @return 仅包含活跃通道的映射表
	 */
	public Map<String, ChannelHandlerContext> getValidSlaveChannelMap() {
		// 创建存储无效通道ID的列表
		List<String> inValidChannelReqIdList = new ArrayList<>();

		// 遍历检查所有通道的活跃状态
		for (String reqId : channelHandlerContextMap.keySet()) {
			// 获取从节点通道
			Channel slaveChannel = channelHandlerContextMap.get(reqId).channel();
			// 检查通道是否活跃，非活跃通道加入待移除列表
			if (!slaveChannel.isActive()) {
				inValidChannelReqIdList.add(reqId);
				continue;
			}
		}

		// 如果存在无效通道，从主映射表中移除
		if (!inValidChannelReqIdList.isEmpty()) {
			for (String reqId : inValidChannelReqIdList) {
				// 移除不可用的channel
				channelHandlerContextMap.remove(reqId);
			}
		}

		// 返回清理后的有效通道映射表
		return channelHandlerContextMap;
	}

	/**
	 * 添加通道映射
	 * <p>
	 * 将指定请求ID与通道上下文的映射关系添加到管理器中。
	 * 如果已存在相同请求ID的映射，将被新的映射覆盖。
	 *
	 * @param reqId 请求ID，通常是从节点的唯一标识
	 * @param channelHandlerContext 与请求ID对应的通道处理上下文
	 */
	public void put(String reqId, ChannelHandlerContext channelHandlerContext) {
		channelHandlerContextMap.put(reqId, channelHandlerContext);
	}

	/**
	 * 获取指定请求ID对应的通道上下文
	 * <p>
	 * 根据请求ID从映射表中检索对应的通道上下文。
	 * 注意：该方法没有返回值，可能是设计缺陷，建议修改为返回检索结果。
	 *
	 * @param reqId 请求ID
	 */
	public void get(String reqId) {
		channelHandlerContextMap.get(reqId);
	}
}
