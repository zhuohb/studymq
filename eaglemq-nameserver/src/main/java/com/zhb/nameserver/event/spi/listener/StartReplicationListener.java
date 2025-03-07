package com.zhb.nameserver.event.spi.listener;

import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.StartReplicationEvent;
import com.zhb.nameserver.utils.NameserverUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import com.zhb.common.enums.NameServerResponseCode;
import com.zhb.common.event.Listener;

import java.net.InetSocketAddress;

/**
 * 主从复制启动监听器
 * <p>
 * 负处理从节点发起的复制同步连接请求，实现主从架构中的初始连接建立
 * 是主从复制模式的入口点，控制从节点接入集群的身份验证和注册过程
 * <p>
 * 主要功能：
 * 1. 对请求连接的从节点进行身份验证
 * 2. 记录和管理从节点的连接信息
 * 3. 将从节点连接注册到复制连接管理器中
 * 4. 发送确认消息，建立主从复制通道
 * <p>
 * 使用场景：
 * - 从节点首次连接到主节点时的初始化流程
 * - 主从复制模式下的节点注册和认证
 * - 集群扩展时新从节点的接入控制
 * - 确保只有授权的从节点能够加入复制集群
 */
public class StartReplicationListener implements Listener<StartReplicationEvent> {

    /**
     * 处理从节点发起的复制同步连接请求
     * <p>
     * 接收并处理从节点的连接请求，验证身份，记录连接信息，
     * 并建立主从节点间的通道，完成初始连接建立流程。
     * <p>
     * 处理流程：
     * 1. 验证从节点提供的用户名和密码
     * 2. 如果验证失败，返回错误消息并关闭连接
     * 3. 记录从节点的IP和端口信息
     * 4. 生成并设置请求ID作为连接标识
     * 5. 将连接注册到复制连接管理器中
     * 6. 向从节点发送确认消息，完成连接建立
     *
     * @param event 从节点发起的复制启动事件对象
     * @throws Exception 验证失败或处理过程中的异常
     */
    @Override
    public void onReceive(StartReplicationEvent event) throws Exception {
        // 验证从节点提供的用户名和密码
        boolean isVerify = NameserverUtils.isVerify(event.getUser(), event.getPassword());
        ChannelHandlerContext channelHandlerContext = event.getChannelHandlerContext();
        if (!isVerify) {
            // 验证失败，返回错误消息并关闭连接
            TcpMsg tcpMsg = new TcpMsg(NameServerResponseCode.ERROR_USER_OR_PASSWORD.getCode(),
                    NameServerResponseCode.ERROR_USER_OR_PASSWORD.getDesc().getBytes());
            channelHandlerContext.writeAndFlush(tcpMsg);
            channelHandlerContext.close();
            throw new IllegalAccessException("error account to connected!");
        }
        // 获取从节点的IP地址和端口信息
        InetSocketAddress inetSocketAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
        event.setSlaveIp(inetSocketAddress.getHostString());
        event.setSlavePort(String.valueOf(inetSocketAddress.getPort()));
        // 生成请求ID作为连接标识，格式为"IP:端口"
        String reqId = event.getSlaveIp() + ":" + event.getSlavePort();
        // 在通道上设置请求ID属性，用于后续识别
        channelHandlerContext.attr(AttributeKey.valueOf("reqId")).set(reqId);
        // 将从节点连接注册到复制连接管理器中
        CommonCache.getReplicationChannelManager().put(reqId, channelHandlerContext);
        // 向从节点发送确认消息，表明复制连接已建立
        TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.MASTER_START_REPLICATION_ACK.getCode(), new byte[0]);
        channelHandlerContext.writeAndFlush(tcpMsg);
    }
}
