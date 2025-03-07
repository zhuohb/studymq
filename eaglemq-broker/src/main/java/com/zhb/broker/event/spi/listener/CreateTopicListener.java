package com.zhb.broker.event.spi.listener;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.event.model.CreateTopicEvent;
import com.zhb.broker.model.CommitLogModel;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.model.QueueModel;
import com.zhb.broker.utils.LogFileNameUtil;
import com.zhb.common.coder.TcpMsg;
import com.zhb.common.constants.BrokerConstants;
import com.zhb.common.dto.CreateTopicReqDTO;
import com.zhb.common.enums.BrokerClusterModeEnum;
import com.zhb.common.enums.BrokerEventCode;
import com.zhb.common.enums.BrokerResponseCode;
import com.zhb.common.event.Listener;
import com.zhb.common.utils.AssertUtils;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主题创建监听器
 * 负责处理创建主题的请求，创建相关文件结构并将主题加入系统缓存
 * 在集群模式下，主节点还需要将创建请求同步到从节点
 */
@Slf4j
public class CreateTopicListener implements Listener<CreateTopicEvent> {

	/**
	 * 接收并处理创建主题事件
	 * 验证请求参数，创建文件结构，并在缓存中注册新主题
	 *
	 * @param event 创建主题事件对象
	 * @throws Exception 处理过程中可能发生的异常
	 */
	@Override
	public void onReceive(CreateTopicEvent event) throws Exception {
		// 获取创建主题请求数据
		CreateTopicReqDTO createTopicReqDTO = event.getCreateTopicReqDTO();

		// 验证队列数量参数的合法性
		AssertUtils.isTrue(createTopicReqDTO.getQueueSize() > 0 && createTopicReqDTO.getQueueSize() < 100, "queueSize参数异常");

		// 检查主题是否已存在
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(createTopicReqDTO.getTopic());
		AssertUtils.isTrue(eagleMqTopicModel == null, "topic已经存在");

		// 创建主题相关文件结构
		createTopicFile(createTopicReqDTO);

		// 在系统缓存中添加主题信息
		addTopicInCommonCache(createTopicReqDTO);

		// 将文件加载到内存映射中
		loadFileInMMap(createTopicReqDTO);
		log.info("topic:{} is created! queueSize is {}", createTopicReqDTO.getTopic(), createTopicReqDTO.getQueueSize());

		// 发送创建成功响应
		event.getChannelHandlerContext().write(new TcpMsg(BrokerResponseCode.CREATED_TOPIC_SUCCESS.getCode(), "success".getBytes()));

		// 集群模式下的主从同步处理
		if (BrokerClusterModeEnum.MASTER_SLAVE.getCode().equals(CommonCache.getGlobalProperties().getBrokerClusterMode())
			&& "master".equals(CommonCache.getGlobalProperties().getBrokerClusterRole())) {
			// 如果是主从模式且当前节点是主节点，将创建主题请求同步给所有从节点
			for (ChannelHandlerContext slaveChannel : CommonCache.getSlaveChannelMap().values()) {
				slaveChannel.writeAndFlush(new TcpMsg(BrokerEventCode.CREATE_TOPIC.getCode(), JSON.toJSONBytes(createTopicReqDTO)));
			}
		}
	}

	/**
	 * 创建主题对应的文件结构
	 * 包括创建CommitLog目录、文件以及ConsumeQueue目录和文件
	 *
	 * @param createTopicReqDTO 创建主题请求数据
	 * @throws IOException 文件操作异常
	 */
	public static void createTopicFile(CreateTopicReqDTO createTopicReqDTO) throws IOException {
		// 创建CommitLog目录和初始文件
		String baseCommitLogDirPath = LogFileNameUtil.buildCommitLogBasePath(createTopicReqDTO.getTopic());
		File commitLogDir = new File(baseCommitLogDirPath);
		commitLogDir.mkdir();
		File commitLogFile = new File(baseCommitLogDirPath + BrokerConstants.SPLIT + LogFileNameUtil.buildFirstCommitLogName());
		commitLogFile.createNewFile();

		// 创建ConsumeQueue目录和各队列的初始文件
		String baseConsumeQueueDirPath = LogFileNameUtil.buildConsumeQueueBasePath(createTopicReqDTO.getTopic());
		File consumeQueueDir = new File(baseConsumeQueueDirPath);
		consumeQueueDir.mkdir();
		// 根据队列数量创建对应数量的队列目录和初始文件
		for (int i = 0; i < createTopicReqDTO.getQueueSize(); i++) {
			new File(baseConsumeQueueDirPath + BrokerConstants.SPLIT + i).mkdir();
			new File(baseConsumeQueueDirPath + BrokerConstants.SPLIT + i + BrokerConstants.SPLIT + LogFileNameUtil.buildFirstConsumeQueueName())
				.createNewFile();
		}
	}

	/**
	 * 加载文件到内存映射（MMap）中
	 * 准备CommitLog和ConsumeQueue的内存映射
	 *
	 * @param createTopicReqDTO 创建主题请求数据
	 * @throws IOException 文件加载异常
	 */
	public static void loadFileInMMap(CreateTopicReqDTO createTopicReqDTO) throws IOException {
		// 准备主题的CommitLog内存映射
		CommonCache.getCommitLogAppendHandler().prepareMMapLoading(createTopicReqDTO.getTopic());
		// 准备主题的ConsumeQueue内存映射
		CommonCache.getConsumeQueueAppendHandler().prepareConsumeQueue(createTopicReqDTO.getTopic());
	}


	/**
	 * 将主题信息添加到系统缓存中
	 * 创建主题模型和队列模型，并添加到全局缓存
	 *
	 * @param createTopicReqDTO 创建主题请求数据
	 */
	public static void addTopicInCommonCache(CreateTopicReqDTO createTopicReqDTO) {
		// 创建主题模型
		EagleMqTopicModel eagleMqTopicModel = new EagleMqTopicModel();
		eagleMqTopicModel.setTopic(createTopicReqDTO.getTopic());

		// 设置创建和更新时间戳
		long currentTimeStamp = System.currentTimeMillis();
		eagleMqTopicModel.setCreateAt(currentTimeStamp);
		eagleMqTopicModel.setUpdateAt(currentTimeStamp);

		// 创建CommitLog模型
		CommitLogModel commitLogModel = new CommitLogModel();
		commitLogModel.setFileName(LogFileNameUtil.buildFirstCommitLogName());
		commitLogModel.setOffsetLimit(BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE.longValue());
		commitLogModel.setOffset(new AtomicInteger(0));
		eagleMqTopicModel.setCommitLogModel(commitLogModel);

		// 创建队列模型列表
		List<QueueModel> queueList = new ArrayList<>();
		for (int i = 0; i < createTopicReqDTO.getQueueSize(); i++) {
			QueueModel queueModel = new QueueModel();
			queueModel.setId(i);
			queueModel.setFileName(LogFileNameUtil.buildFirstConsumeQueueName());
			queueModel.setOffsetLimit(BrokerConstants.COMSUMEQUEUE_DEFAULT_MMAP_SIZE);
			queueModel.setLastOffset(0);
			queueModel.setLatestOffset(new AtomicInteger(0));
			queueList.add(queueModel);
		}
		// 设置队列列表并将主题模型添加到全局缓存
		eagleMqTopicModel.setQueueList(queueList);
		CommonCache.getEagleMqTopicModelList().add(eagleMqTopicModel);
	}

}
