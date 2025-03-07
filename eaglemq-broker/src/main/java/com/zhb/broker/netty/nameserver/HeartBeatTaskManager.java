package com.zhb.broker.netty.nameserver;

import com.alibaba.fastjson2.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.HeartBeatDTO;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.remote.NameServerNettyRemoteClient;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 心跳任务管理器
 * 负责创建并管理Broker向NameServer发送心跳包的定时任务
 * 通过心跳包维持Broker与NameServer的活跃连接，便于注册中心监控Broker存活状态
 */
@Slf4j
public class HeartBeatTaskManager {
	/**
	 * 任务启动标志
	 * 使用原子整数确保心跳任务只会被启动一次
	 */
	private final AtomicInteger flag = new AtomicInteger(0);

	/**
	 * 启动心跳传输任务
	 * 创建并启动一个专用线程，定期发送心跳包到NameServer
	 * 通过原子标志确保任务只会被启动一次
	 */
	public void startTask() {
		if (flag.getAndIncrement() >= 1) {
			return;
		}
		Thread heartBeatRequestTask = new Thread(new HeartBeatRequestTask());
		heartBeatRequestTask.setName("heart-beat-request-task");
		heartBeatRequestTask.start();
	}

	/**
	 * 心跳请求任务
	 * 内部类实现Runnable接口，定期向NameServer发送心跳消息
	 * 心跳消息用于维持Broker在NameServer中的注册状态
	 */
	private class HeartBeatRequestTask implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					// 每隔3秒发送一次心跳
					TimeUnit.SECONDS.sleep(3);

					// 心跳包不需要额外透传过多的参数，只需要告诉nameserver这个channel依然存活即可
					NameServerNettyRemoteClient nameServerNettyRemoteClient = CommonCache.getNameServerClient().getNameServerNettyRemoteClient();

					// 构建心跳数据对象
					HeartBeatDTO heartBeatDTO = new HeartBeatDTO();
					heartBeatDTO.setMsgId(UUID.randomUUID().toString());

					// 创建TCP消息并发送
					TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.HEART_BEAT.getCode(), JSON.toJSONBytes(heartBeatDTO));
					TcpMsg heartBeatResp = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg, heartBeatDTO.getMsgId());

					// 检查心跳响应状态
					if (NameServerResponseCode.HEART_BEAT_SUCCESS.getCode() != heartBeatResp.getCode()) {
						log.error("heart beat from nameserver is error:{}", heartBeatResp.getCode());
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
