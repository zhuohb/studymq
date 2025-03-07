package com.zhb.nameserver.event.spi.listener;

import com.zhb.common.event.Listener;
import com.zhb.nameserver.event.model.SlaveHeartBeatEvent;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class SlaveHeartBeatListener implements Listener<SlaveHeartBeatEvent> {

	@Override
	public void onReceive(SlaveHeartBeatEvent event) throws Exception {
		log.info("接收到从节点心跳信号");
	}
}
