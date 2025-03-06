package com.zhb.client.consumer;


import com.alibaba.fastjson.JSON;
import com.zhb.client.netty.BrokerRemoteRespHandler;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.dto.CreateTopicReqDTO;
import com.zhb.common.enums.BrokerEventCode;
import com.zhb.common.event.EventBus;
import com.zhb.common.remote.BrokerNettyRemoteClient;

/**
 * @Author idea
 * @Date: Created at 2024/7/7
 * @Description 创建topic的class
 */
public class CreateTopicCommand {


    public static void main(String[] args) {
        BrokerNettyRemoteClient brokerNettyRemoteClient = new BrokerNettyRemoteClient("127.0.0.1", 8990);
        brokerNettyRemoteClient.buildConnection(new BrokerRemoteRespHandler(new EventBus("mq-client-eventbus")));
        CreateTopicReqDTO createTopicReqDTO = new CreateTopicReqDTO();
        createTopicReqDTO.setTopic("delay_queue");
        createTopicReqDTO.setQueueSize(1);
        brokerNettyRemoteClient.sendAsyncMsg(new TcpMsg(BrokerEventCode.CREATE_TOPIC.getCode(), JSON.toJSONBytes(createTopicReqDTO)));
        System.out.println("已经创建topic：" + createTopicReqDTO.getTopic());
    }
}
