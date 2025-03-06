package com.zhb.broker.netty.nameserver;

import com.alibaba.fastjson2.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.HeartBeatDTO;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.remote.NameServerNettyRemoteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author idea
 * @Date: Created in 08:51 2024/5/12
 * @Description 心跳数据上报任务
 */
public class HeartBeatTaskManager {

    private static Logger logger = LoggerFactory.getLogger(HeartBeatTaskManager.class);
    private AtomicInteger flag = new AtomicInteger(0);

    //开启心跳传输任务
    public void startTask() {
        if (flag.getAndIncrement() >= 1) {
            return;
        }
        Thread heartBeatRequestTask = new Thread(new HeartBeatRequestTask());
        heartBeatRequestTask.setName("heart-beat-request-task");
        heartBeatRequestTask.start();
    }


    private class HeartBeatRequestTask implements Runnable{
        @Override
        public void run() {
            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(3);
                    //心跳包不需要额外透传过多的参数，只需要告诉nameserver这个channel依然存活即可
                    NameServerNettyRemoteClient nameServerNettyRemoteClient = CommonCache.getNameServerClient().getNameServerNettyRemoteClient();
                    HeartBeatDTO heartBeatDTO = new HeartBeatDTO();
                    heartBeatDTO.setMsgId(UUID.randomUUID().toString());
                    TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.HEART_BEAT.getCode(), JSON.toJSONBytes(heartBeatDTO));
                    TcpMsg heartBeatResp = nameServerNettyRemoteClient.sendSyncMsg(tcpMsg,heartBeatDTO.getMsgId());
                    if(NameServerResponseCode.HEART_BEAT_SUCCESS.getCode() != heartBeatResp.getCode()) {
                        logger.error("heart beat from nameserver is error:{}",heartBeatResp.getCode());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
