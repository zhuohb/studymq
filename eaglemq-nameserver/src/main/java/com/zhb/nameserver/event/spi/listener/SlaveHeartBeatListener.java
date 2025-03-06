package com.zhb.nameserver.event.spi.listener;

import com.zhb.nameserver.event.model.SlaveHeartBeatEvent;
import com.zhb.common.event.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author idea
 * @Date: Created in 08:57 2024/5/22
 * @Description
 */
public class SlaveHeartBeatListener implements Listener<SlaveHeartBeatEvent> {

	private final Logger logger = LoggerFactory.getLogger(SlaveHeartBeatListener.class);

	@Override
	public void onReceive(SlaveHeartBeatEvent event) throws Exception {
		logger.info("接收到从节点心跳信号");
	}
}
