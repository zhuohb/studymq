package com.zhb.nameserver.core;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.store.ServiceInstance;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author idea
 * @Date: Created in 22:52 2024/5/4
 * @Description 移除非正常服务任务
 */
public class InValidServiceRemoveTask implements Runnable {


	@Override
	public void run() {
		while (true) {
			try {
				TimeUnit.SECONDS.sleep(3);
				Map<String, ServiceInstance> serviceInstanceMap = CommonCache.getServiceInstanceManager().getServiceInstanceMap();
				long currentTime = System.currentTimeMillis();
				Iterator<String> iterator = serviceInstanceMap.keySet().iterator();
				while (iterator.hasNext()) {
					String brokerReqId = iterator.next();
					ServiceInstance serviceInstance = serviceInstanceMap.get(brokerReqId);
					if (serviceInstance.getLastHeartBeatTime() == null) {
						continue;
					}
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
