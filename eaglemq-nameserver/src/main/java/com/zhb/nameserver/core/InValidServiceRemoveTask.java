package com.zhb.nameserver.core;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.store.ServiceInstance;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 无效服务实例清理任务
 * <p>
 * 该任务负责定期检查并清理NameServer中已注册但长时间未发送心跳的服务实例
 * 通过心跳超时机制，确保NameServer维护的服务实例列表始终是活跃可用的
 * <p>
 * 工作原理：
 * 1. 以固定间隔（3秒）运行检查循环
 * 2. 遍历所有注册的服务实例
 * 3. 对比当前时间与最后心跳时间
 * 4. 移除超过心跳超时阈值（9秒）的服务实例
 * <p>
 * 该任务是保证NameServer服务发现准确性的关键组件，防止客户端连接到已失效的服务节点
 */
public class InValidServiceRemoveTask implements Runnable {

	/**
	 * 执行无效服务实例清理任务
	 * <p>
	 * 以固定间隔循环执行，检查服务实例的心跳时间，移除超时的实例
	 * 心跳超时阈值为3个心跳周期（约9秒），超过此阈值的服务实例将被视为不可用并从注册表中移除
	 */
	@Override
	public void run() {
		while (true) {
			try {
				// 等待3秒后执行检查，控制检查频率
				TimeUnit.SECONDS.sleep(3);

				// 获取当前注册的所有服务实例
				Map<String, ServiceInstance> serviceInstanceMap = CommonCache.getServiceInstanceManager().getServiceInstanceMap();
				long currentTime = System.currentTimeMillis();

				// 使用迭代器遍历服务实例，以便安全地移除过期实例
				Iterator<String> iterator = serviceInstanceMap.keySet().iterator();
				while (iterator.hasNext()) {
					String brokerReqId = iterator.next();
					ServiceInstance serviceInstance = serviceInstanceMap.get(brokerReqId);

					// 跳过尚未发送过心跳的服务实例（可能是刚注册的）
					if (serviceInstance.getLastHeartBeatTime() == null) {
						continue;
					}

					// 检查心跳是否超时（超过9秒未更新）
					// 超时则从服务注册表中移除该实例
					if (currentTime - serviceInstance.getLastHeartBeatTime() > 3000 * 3) {
						iterator.remove();
					}
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
