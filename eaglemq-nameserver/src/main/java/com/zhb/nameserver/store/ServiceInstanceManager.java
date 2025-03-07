package com.zhb.nameserver.store;

import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务实例管理器
 * <p>
 * 该类负责管理NameServer中注册的所有服务实例，是服务发现和负载均衡的核心组件。
 * 维护着服务实例的注册信息、心跳状态和元数据，支持服务的注册、发现、心跳更新和注销功能。
 * <p>
 * 功能特点：
 * 1. 使用线程安全的ConcurrentHashMap存储服务实例，支持高并发访问
 * 2. 提供服务实例的增删改查操作，满足服务发现和管理需求
 * 3. 支持服务实例的心跳更新，用于判断服务健康状态
 * 4. 支持批量重载服务实例数据，用于系统初始化或数据恢复场景
 * <p>
 * 应用场景：
 * - 服务注册与发现
 * - 服务健康状态监控
 * - 集群节点管理
 */
@Getter
public class ServiceInstanceManager {

	/**
	 * 服务实例映射表
	 * <p>
	 * 使用线程安全的ConcurrentHashMap存储服务实例信息，
	 * 键为服务实例的唯一标识符（通常是"IP:端口"格式），
	 * 值为对应的服务实例对象，包含实例的详细信息和状态。
	 * -- GETTER --
	 *  获取完整的服务实例映射表
	 *  <p>
	 *  返回当前管理器中的所有服务实例映射关系，
	 *  可用于服务发现、负载均衡等功能实现。
	 *
	 * @return 当前的服务实例映射表

	 */
	private Map<String, ServiceInstance> serviceInstanceMap = new ConcurrentHashMap<>();

	/**
	 * 更新已存在的服务实例
	 * <p>
	 * 该方法用于处理服务实例的心跳更新请求。
	 * 首先检查目标服务实例是否已注册，如已注册则更新其最近心跳时间；
	 * 如未注册或已被清理，则抛出异常要求服务重新注册。
	 * <p>
	 * 心跳更新���判断服务健康状态的关键机制，也是防止服务被误清理的手段。
	 *
	 * @param serviceInstance 需要更新的服务实例信息
	 * @throws RuntimeException 当服务实例不存在或已被清理时抛出
	 */
	public void putIfExist(ServiceInstance serviceInstance) {
		ServiceInstance currentInstance = this.get(serviceInstance.getIp(), serviceInstance.getPort());
		if (currentInstance != null && currentInstance.getFirstRegistryTime() != null) {
			// 服务实例存在，更新最近心跳时间
			currentInstance.setLastHeartBeatTime(serviceInstance.getLastHeartBeatTime());
			serviceInstanceMap.put(serviceInstance.getIp() + ":" + serviceInstance.getPort(), currentInstance);
		} else {
			// 服务实例不存在或已被清理，要求重新注册
			throw new RuntimeException("之前心跳缓存已经剔除，请重新注册");
		}
	}

	/**
	 * 批量重载服务实例数据
	 * <p>
	 * 清空当前的所有服务实例记录，并使用提供的服务实例列表重新填充。
	 * 此操作通常用于系统初始化、数据恢复或配置变更场景。
	 * <p>
	 * 注意: 该方法有待添加并发控制机制，确保在重载过程中的数据一致性
	 *
	 * @param serviceInstanceList 新的服务实例列表
	 */
	//todo 加锁
	public void reload(List<ServiceInstance> serviceInstanceList) {
		serviceInstanceMap.clear();
		for (ServiceInstance serviceInstance : serviceInstanceList) {
			this.put(serviceInstance);
		}
	}

	/**
	 * 添加或更新服务实例
	 * <p>
	 * 将提供的服务实例添加到管理器中，如果已存在相同IP:端口的实例，
	 * 则更新其信息。该方法是服务注册的核心实现。
	 *
	 * @param serviceInstance 需要添加或更新的服务实例
	 */
	public void put(ServiceInstance serviceInstance) {
		serviceInstanceMap.put(serviceInstance.getIp() + ":" + serviceInstance.getPort(), serviceInstance);
	}

	/**
	 * 通过IP和端口获取服务实例
	 * <p>
	 * 根据指定的IP地址和端口号，查找并返回对应的服务实例。
	 * ���果指定的服务实例不存在，则返回null。
	 *
	 * @param brokerIp 服务实例的IP地址
	 * @param brokerPort 服务实例的端口号
	 * @return 对应的服务实例，不存在则返回null
	 */
	public ServiceInstance get(String brokerIp, Integer brokerPort) {
		return serviceInstanceMap.get(brokerIp + ":" + brokerPort);
	}

	/**
	 * 通过请求ID获取服务实例
	 * <p>
	 * 请求ID通常是形如"IP:端口"的字符串，用于唯一标识一个服务实例。
	 * 此方法用于根据该标识符直接获取对应的服务实例。
	 *
	 * @param reqId 请求ID，通常是"IP:端口"格式
	 * @return 对应的服务实例，不存在则返回null
	 */
	public ServiceInstance get(String reqId) {
		return serviceInstanceMap.get(reqId);
	}

	/**
	 * 移除服务实例
	 * <p>
	 * 根据提供的键（通常是"IP:端口"格式）移除对应的服务实例。
	 * 此方法用于服务注销、实例失效清理等场景。
	 *
	 * @param key 服务实例的键，通常是"IP:端口"格式
	 * @return 被移除的服务实例，如果键不存在则返回null
	 */
	public ServiceInstance remove(String key) {
		return serviceInstanceMap.remove(key);
	}

}
