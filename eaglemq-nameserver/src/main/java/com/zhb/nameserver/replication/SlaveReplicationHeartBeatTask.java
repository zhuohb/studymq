package com.zhb.nameserver.replication;

import com.alibaba.fastjson.JSON;
import com.zhb.nameserver.common.CommonCache;
import com.zhb.nameserver.event.model.SlaveHeartBeatEvent;
import com.zhb.nameserver.event.model.StartReplicationEvent;
import io.netty.channel.Channel;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.enums.NameServerEventCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 从节点心跳发送任务
 * <p>
 * 该任务负责从��点向主节点定期发送心跳消息，是主从复制架构中保持节点连接活跃性和检测节点状态的关键组件。
 * 心跳机制确保主节点能够及时检测从节点的在线状态，是实现高可用性和故障转移的基础。
 * <p>
 * 工作流程：
 * 1. 初始启动时，向主节点发送认证和复制启动请求
 * 2. 之后以固定间隔（默认3秒）向主节点发送心跳消息
 * 3. 持续运行，直到任务被终止或发生异常
 * <p>
 * 心跳机制的作用：
 * - 维持主从节点间的连接活跃性
 * - 让主节点能够检测从节点是否在线
 * - 支持主节点维护有效从节点列表，用于复制决策
 */
public class SlaveReplicationHeartBeatTask extends ReplicationTask {

	/**
	 * 日志记录器，用于记录心跳发送过程中的状态和异常
	 */
	private final Logger logger = LoggerFactory.getLogger(SlaveReplicationHeartBeatTask.class);

	/**
	 * 创建从节点心跳发送任务
	 *
	 * @param taskName 任务名称，用于标识和日志记录
	 */
	public SlaveReplicationHeartBeatTask(String taskName) {
		super(taskName);
	}

	/**
	 * 启动从节点心跳任务
	 * <p>
	 * 该方法实现了从节点向主节点发送心跳的完整流程。包含两个主要部分：
	 * 1. 初始化阶段：发送认证和复制启动请求，建立与主节点的复制关系
	 * 2. 心跳维护阶段：以固定间隔循环发送心跳消息，维持连接活跃性
	 * <p>
	 * 任务启动后会持续运行，直到线程被中断或应用关闭。心跳发送失败（如连接断开）
	 * 会抛出异常并终止任务，需由外部监控机制重启或触发故障转移。
	 */
	@Override
	public void startTask() {
		// 初始化延迟，给系统一定时间完成启动和网络连接
		try {
			// 启动任务3秒后发送第一条消息，避免过早发送导致连接还未就绪
			TimeUnit.SECONDS.sleep(3);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		// 创建复制启动事件，包含从节点认证凭据
		StartReplicationEvent startReplicationEvent = new StartReplicationEvent();
		// 设置用户名，用于主节点验证从节点身份
		startReplicationEvent.setUser(CommonCache.getNameserverProperties().getNameserverUser());
		// 设置密码，用于主节点验证从节点身份
		startReplicationEvent.setPassword(CommonCache.getNameserverProperties().getNameserverPwd());

		// 封装为TCP消息，指定事件类型为启动复制
		TcpMsg startReplicationMsg = new TcpMsg(NameServerEventCode.START_REPLICATION.getCode(),
			JSON.toJSONBytes(startReplicationEvent));

		// 通过建立好的通道发送启动复制消息到主节点
		CommonCache.getConnectNodeChannel().writeAndFlush(startReplicationMsg);

		// 心跳维护循环，持续发送心跳消息
		while (true) {
			try {
				// 每隔3秒发送一次心跳，间隔可根据系统需求调整
				TimeUnit.SECONDS.sleep(3);

				// 获取与主节点的通信通道
				Channel channel = CommonCache.getConnectNodeChannel();

				// 创建心跳消息，事件类型为从节点心跳
				TcpMsg tcpMsg = new TcpMsg(NameServerEventCode.SLAVE_HEART_BEAT.getCode(),
					JSON.toJSONBytes(new SlaveHeartBeatEvent()));

				// 发送心跳消息到主节点
				channel.writeAndFlush(tcpMsg);

				// 记录心跳发送日志，便于监控和故障排查
				logger.info("从节点发送心跳数据给master");
			} catch (InterruptedException e) {
				// 如果线程被中断，终止心跳任务
				throw new RuntimeException(e);
			}
		}
	}
}
