package com.zhb.broker.core;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.*;
import com.zhb.broker.utils.AckMessageLock;
import com.zhb.broker.utils.UnfailReentrantLock;
import com.zhb.common.dto.ConsumeMsgCommitLogDTO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费队列消费处理器
 * 负责消费者从消费队列中获取消息及确认消费的相关操作
 * 提供批量消费和消费确认（ACK）功能
 */
public class ConsumeQueueConsumeHandler {
	/**
	 * 消息确认锁
	 * 用于保证消息确认（ACK）操作的线程安全
	 */
	public AckMessageLock ackMessageLock = new UnfailReentrantLock();

	/**
	 * 消费指定数量的消息
	 * 从消费队列中读取当前最新的N条消息内容，并返回CommitLog中的原始数据
	 *
	 * @param consumeQueueConsumeReqModel 消费请求模型，包含主题、消费组、队列ID和批次大小等信息
	 * @return 消费消息列表，包含从CommitLog中读取的原始消息内容
	 */
	public List<ConsumeMsgCommitLogDTO> consume(ConsumeQueueConsumeReqModel consumeQueueConsumeReqModel) {
		String topic = consumeQueueConsumeReqModel.getTopic();
		//1.检查参数合法性
		//2.获取当前匹配的队列的最新的consumeQueue的offset是多少
		//3.获取当前匹配的队列存储文件的mmap对象，然后读取offset地址的数据
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		if (eagleMqTopicModel == null) {
			throw new RuntimeException("topic " + topic + " not exist!");
		}
		String consumeGroup = consumeQueueConsumeReqModel.getConsumeGroup();
		Integer queueId = consumeQueueConsumeReqModel.getQueueId();
		Integer batchSize = consumeQueueConsumeReqModel.getBatchSize();
		// 获取消费队列偏移量管理模型
		ConsumeQueueOffsetModel.OffsetTable offsetTable = CommonCache.getConsumeQueueOffsetModel().getOffsetTable();
		Map<String, ConsumeQueueOffsetModel.ConsumerGroupDetail> consumerGroupDetailMap = offsetTable.getTopicConsumerGroupDetail();
		ConsumeQueueOffsetModel.ConsumerGroupDetail consumerGroupDetail = consumerGroupDetailMap.get(topic);
		// 如果是首次消费，初始化消费者组详情
		if (consumerGroupDetail == null) {
			consumerGroupDetail = new ConsumeQueueOffsetModel.ConsumerGroupDetail();
			consumerGroupDetailMap.put(topic, consumerGroupDetail);
		}
		// 获取消费组的偏移量详情
		Map<String, Map<String, String>> consumeGroupOffsetMap = consumerGroupDetail.getConsumerGroupDetailMap();
		Map<String, String> queueOffsetDetailMap = consumeGroupOffsetMap.get(consumeGroup);
		List<QueueModel> queueList = eagleMqTopicModel.getQueueList();
		// 如果是该消费组首次消费，初始化所有队列的偏移量为起始位置
		if (queueOffsetDetailMap == null) {
			queueOffsetDetailMap = new HashMap<>();
			for (QueueModel queueModel : queueList) {
				queueOffsetDetailMap.put(String.valueOf(queueModel.getId()), "00000000#0");
			}
			consumeGroupOffsetMap.put(consumeGroup, queueOffsetDetailMap);
		}
		// 获取当前队列的消费偏移量信息
		String offsetStrInfo = queueOffsetDetailMap.get(String.valueOf(queueId));
		String[] offsetStrArr = offsetStrInfo.split("#");
		int consumeQueueOffset = Integer.parseInt(offsetStrArr[1]);
		QueueModel queueModel = queueList.get(queueId);
		// 如果已消费到队列尽头，返回null
		if (queueModel.getLatestOffset().get() <= consumeQueueOffset) {
			return null;
		}
		// 获取消费队列内存映射模型并读取指定偏移量的消息内容
		List<ConsumeQueueMMapFileModel> consumeQueueOffsetModels = CommonCache.getConsumeQueueMMapFileModelManager().get(topic);
		ConsumeQueueMMapFileModel consumeQueueMMapFileModel = consumeQueueOffsetModels.get(queueId);
		//一次读取多条consumeQueue的数据内容
		List<byte[]> consumeQueueContentList = consumeQueueMMapFileModel.readContent(consumeQueueOffset, batchSize);
		List<ConsumeMsgCommitLogDTO> commitLogBodyContentList = new ArrayList<>();
		// 根据消费队列中的索引信息，从CommitLog中读取实际消息内容
		for (byte[] content : consumeQueueContentList) {
			// 根据consumeQueue的内容确定commitLog的读取位置
			ConsumeQueueDetailModel consumeQueueDetailModel = new ConsumeQueueDetailModel();
			consumeQueueDetailModel.buildFromBytes(content);
			CommitLogMMapFileModel commitLogMMapFileModel = CommonCache.getCommitLogMMapFileModelManager().get(topic);
			ConsumeMsgCommitLogDTO commitLogContent = commitLogMMapFileModel.readContent(consumeQueueDetailModel.getMsgIndex(), consumeQueueDetailModel.getMsgLength());
			commitLogContent.setRetryTimes(consumeQueueDetailModel.getRetryTimes());
			commitLogBodyContentList.add(commitLogContent);
		}

		return commitLogBodyContentList;
	}

	/**
	 * 确认消息已消费（ACK）
	 * 更新指定主题、消费组和队列的消费偏移量
	 *
	 * @param topic        主题名称
	 * @param consumeGroup 消费组名称
	 * @param queueId      队列ID
	 * @return 确认结果，true表示确认成功，false表示确认失败
	 */
	public boolean ack(String topic, String consumeGroup, Integer queueId) {
		try {
			// 获取偏移量表并更新指定队列的消费偏移量
			ConsumeQueueOffsetModel.OffsetTable offsetTable = CommonCache.getConsumeQueueOffsetModel().getOffsetTable();
			Map<String, ConsumeQueueOffsetModel.ConsumerGroupDetail> consumerGroupDetailMap = offsetTable.getTopicConsumerGroupDetail();
			ConsumeQueueOffsetModel.ConsumerGroupDetail consumerGroupDetail = consumerGroupDetailMap.get(topic);
			Map<String, String> consumeQueueOffsetDetailMap = consumerGroupDetail.getConsumerGroupDetailMap().get(consumeGroup);
			// 解析当前偏移量信息并增加一个消费队列条目的大小（16字节）
			String offsetStrInfo = consumeQueueOffsetDetailMap.get(String.valueOf(queueId));
			String[] offsetStrArr = offsetStrInfo.split("#");
			String fileName = offsetStrArr[0];
			Integer currentOffset = Integer.valueOf(offsetStrArr[1]);
			// 每个消费队列条目固定16字节
			currentOffset += 16;
			// 更新消费偏移量
			consumeQueueOffsetDetailMap.put(String.valueOf(queueId), fileName + "#" + currentOffset);
		} catch (Exception e) {
			System.err.println("ack操作异常");
			e.printStackTrace();
		} finally {
			// 空的finally块，预留给未来可能的资源释放或锁操作
		}
		return true;
	}
}
