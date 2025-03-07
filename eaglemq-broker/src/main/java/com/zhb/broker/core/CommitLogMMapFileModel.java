package com.zhb.broker.core;

import com.alibaba.fastjson.JSON;
import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.*;
import com.zhb.broker.utils.LogFileNameUtil;
import com.zhb.broker.utils.PutMessageLock;
import com.zhb.broker.utils.UnfailReentrantLock;
import com.zhb.common.constants.BrokerConstants;
import com.zhb.common.dto.ConsumeMsgCommitLogDTO;
import com.zhb.common.dto.MessageDTO;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * CommitLog内存映射文件模型
 * 负责管理消息在CommitLog文件中的持久化存储，通过内存映射实现高性能读写
 */
@Slf4j
public class CommitLogMMapFileModel {
	/**
	 * CommitLog文件对象
	 */
	private File file;
	/**
	 * 内存映射缓冲区，用于写入操作
	 */
	private MappedByteBuffer mappedByteBuffer;
	/**
	 * 读取操作使用的字节缓冲区
	 */
	private ByteBuffer readByteBuffer;
	/**
	 * CommitLog文件通道
	 */
	private FileChannel fileChannel;
	/**
	 * 当前文件对应的主题名称
	 */
	private String topic;
	/**
	 * 写入消息时使用的锁，保证线程安全
	 */
	private PutMessageLock putMessageLock;


	/**
	 * 指定offset做文件的内存映射
	 * 将CommitLog文件映射到内存中，便于快速读写操作
	 *
	 * @param topicName   消息主题名称
	 * @param startOffset 开始映射的偏移量位置
	 * @param mappedSize  映射的大小 (字节)
	 * @throws IOException 如果映射操作失败
	 */
	public void loadFileInMMap(String topicName, int startOffset, int mappedSize) throws IOException {
		this.topic = topicName;
		String filePath = getLatestCommitLogFile(topicName);
		this.doMMap(filePath, startOffset, mappedSize);
		//默认非公平
		putMessageLock = new UnfailReentrantLock();
	}

	/**
	 * 执行内存映射操作
	 * 将指定的CommitLog文件映射到内存中
	 *
	 * @param filePath    CommitLog文件路径
	 * @param startOffset 开始映射的偏移量
	 * @param mappedSize  映射的大小
	 * @throws IOException 如果文件操作失败
	 */
	private void doMMap(String filePath, int startOffset, int mappedSize) throws IOException {
		file = new File(filePath);
		if (!file.exists()) {
			throw new FileNotFoundException("filePath is " + filePath + " inValid");
		}
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
		this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
		//从指定位置开始追加写入
		this.readByteBuffer = mappedByteBuffer.slice();
		this.mappedByteBuffer.position(eagleMqTopicModel.getCommitLogModel().getOffset().get());
	}

	/**
	 * 获取最新的CommitLog文件路径
	 * 根据主题配置获取当前可用的CommitLog文件路径，如果需要则创建新文件
	 *
	 * @param topicName 主题名称
	 * @return CommitLog文件的完整路径
	 */
	private String getLatestCommitLogFile(String topicName) {
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topicName);
		if (eagleMqTopicModel == null) {
			throw new IllegalArgumentException("topic is inValid! topicName is " + topicName);
		}
		CommitLogModel commitLogModel = eagleMqTopicModel.getCommitLogModel();
		long diff = commitLogModel.countDiff();
		String filePath = null;
		if (diff == 0) {
			//已经写满了 需要创建新文件
			CommitLogFilePath commitLogFilePath = this.createNewCommitLogFile(topicName, commitLogModel);
			filePath = commitLogFilePath.getFilePath();
		} else if (diff > 0) {
			//还有空间可写入
			filePath = LogFileNameUtil.buildCommitLogFilePath(topicName, commitLogModel.getFileName());
		}
		return filePath;
	}


	/**
	 * 高性能写入API
	 * 默认不强制刷盘的消息写入方法
	 *
	 * @param messageDTO 待写入的消息数据传输对象
	 * @throws IOException 如果写入操作失败
	 */
	public void writeContent(MessageDTO messageDTO) throws IOException {
		this.writeContent(messageDTO, false);
	}

	/**
	 * 写入数据到CommitLog文件
	 * 将消息内容写入CommitLog文件，并处理文件容量检查、写满后的文件切换等逻辑
	 *
	 * @param messageDTO 待写入的消息数据传输对象
	 * @param force      是否强制刷盘，true表示立即刷盘，false表示由操作系统决定
	 * @throws IOException 如果写入操作失败
	 */
	public void writeContent(MessageDTO messageDTO, boolean force) throws IOException {
		//定位到最新的commitLog文件中，记录下当前文件是否已经写满，如果写满，则创建新的文件，并且做新的mmap映射
		//如果当前文件没有写满，对content内容做一层封装
		//再判断写入是否会导致commitLog写满，如果不会，则选择当前commitLog，如果会则创建新文件，并且做mmap映射
		//定位到最新的commitLog文件之后，写入

		//定义一个对象专门管理各个topic的最新写入offset值，并且定时刷新到磁盘中
		//写入数据，offset变更，如果是高并发场景，offset是不是会被多个线程访问？

		//offset会用一个原子类AtomicLong去管理
		//线程安全问题：线程1：111，线程2：122
		//加锁机制(锁的选择非常重要）
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		if (eagleMqTopicModel == null) {
			throw new IllegalArgumentException("eagleMqTopicModel is null");
		}
		CommitLogModel commitLogModel = eagleMqTopicModel.getCommitLogModel();
		if (commitLogModel == null) {
			throw new IllegalArgumentException("commitLogModel is null");
		}
		//默认刷到page cache中，
		//如果需要强制刷盘，这里要兼容
		putMessageLock.lock();
		CommitLogMessageModel commitLogMessageModel = new CommitLogMessageModel();
		commitLogMessageModel.setContent(messageDTO.getBody());
		this.checkCommitLogHasEnableSpace(commitLogMessageModel);
		byte[] writeContent = commitLogMessageModel.convertToBytes();
		mappedByteBuffer.put(writeContent);
		AtomicInteger currentLatestMsgOffset = commitLogModel.getOffset();
		this.dispatcher(messageDTO, currentLatestMsgOffset.get());
		currentLatestMsgOffset.addAndGet(writeContent.length);
		if (force) {
			//强制刷盘 确保数据写入物理磁盘
			mappedByteBuffer.force();
		}
		putMessageLock.unlock();
	}

	/**
	 * 从指定位置读取消息内容
	 * 从CommitLog文件的指定偏移量位置读取指定长度的消息内容
	 *
	 * @param pos    起始读取位置
	 * @param length 要读取的字节长度
	 * @return 包含读取内容及元数据的数据传输对象
	 */
	public ConsumeMsgCommitLogDTO readContent(int pos, int length) {
		ByteBuffer readBuf = readByteBuffer.slice();
		readBuf.position(pos);
		byte[] readBytes = new byte[length];
		readBuf.get(readBytes);
		ConsumeMsgCommitLogDTO consumeMsgCommitLogDTO = new ConsumeMsgCommitLogDTO();
		consumeMsgCommitLogDTO.setBody(readBytes);
		consumeMsgCommitLogDTO.setFileName(file.getName());
		consumeMsgCommitLogDTO.setCommitLogOffset(pos);
		consumeMsgCommitLogDTO.setCommitLogSize(length);
		return consumeMsgCommitLogDTO;
	}

	/**
	 * 消息分发处理
	 * 将消息的索引信息写入对应的ConsumeQueue，便于消费者快速定位消息
	 *
	 * @param messageDTO 消息数据传输对象
	 * @param msgIndex   CommitLog中的消息索引位置
	 */
	private void dispatcher(MessageDTO messageDTO, int msgIndex) {
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		if (eagleMqTopicModel == null) {
			throw new RuntimeException("topic is undefined");
		}
		int queueId;
		if (messageDTO.getQueueId() >= 0) {
			queueId = messageDTO.getQueueId();
		} else {
			//todo 后续大家可以在这里自由扩展不同的消息分派策略
			int queueSize = eagleMqTopicModel.getQueueList().size();
			queueId = new Random().nextInt(queueSize);
		}
		ConsumeQueueDetailModel consumeQueueDetailModel = new ConsumeQueueDetailModel();
		consumeQueueDetailModel.setCommitLogFilename(Integer.parseInt(eagleMqTopicModel.getCommitLogModel().getFileName()));
		consumeQueueDetailModel.setMsgIndex(msgIndex);
		consumeQueueDetailModel.setRetryTimes(messageDTO.getCurrentRetryTimes());
		consumeQueueDetailModel.setMsgLength(messageDTO.getBody().length);
		byte[] content = consumeQueueDetailModel.convertToBytes();
		log.info("写入队列数据：{}", JSON.toJSONString(consumeQueueDetailModel));
		List<ConsumeQueueMMapFileModel> queueModelList = CommonCache.getConsumeQueueMMapFileModelManager().get(topic);
		ConsumeQueueMMapFileModel consumeQueueMMapFileModel = queueModelList.stream().filter(queueModel -> queueModel.getQueueId().equals(queueId)).findFirst().orElse(null);
		consumeQueueMMapFileModel.writeContent(content);
		//刷新队列的偏移量
		QueueModel queueModel = eagleMqTopicModel.getQueueList().get(queueId);
		queueModel.getLatestOffset().addAndGet(content.length);
	}

	/**
	 * 检查CommitLog文件是否有足够空间
	 * 如果剩余空间不足以存储新消息，则创建新的CommitLog文件
	 *
	 * @param commitLogMessageModel 待写入的消息模型
	 * @throws IOException 如果文件操作失败
	 */
	private void checkCommitLogHasEnableSpace(CommitLogMessageModel commitLogMessageModel) throws IOException {
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(this.topic);
		CommitLogModel commitLogModel = eagleMqTopicModel.getCommitLogModel();
		long writeAbleOffsetNum = commitLogModel.countDiff();
		//空间不足，需要创建新的commitLog文件并且做映射
		if (!(writeAbleOffsetNum >= commitLogMessageModel.convertToBytes().length)) {
			//00000000文件 -》00000001文件
			//commitLog剩余150byte大小的空间，最新的消息体积是151byte
			CommitLogFilePath commitLogFilePath = this.createNewCommitLogFile(topic, commitLogModel);
			commitLogModel.setOffsetLimit(Long.valueOf(BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE));
			commitLogModel.setOffset(new AtomicInteger(0));
			commitLogModel.setFileName(commitLogFilePath.getFileName());
			//新文件路径映射进来
			this.doMMap(commitLogFilePath.getFilePath(), 0, BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE);
		}
	}

	/**
	 * 释放内存映射资源
	 * 清理MappedByteBuffer占用的直接内存，防止内存泄漏
	 */
	public void clean() {
		if (mappedByteBuffer == null || !mappedByteBuffer.isDirect() || mappedByteBuffer.capacity() == 0) {
			return;
		}
		invoke(invoke(viewed(mappedByteBuffer), "cleaner"), "clean");
	}

	/**
	 * 通过反射调用指定对象的方法
	 *
	 * @param target     目标对象
	 * @param methodName 方法名
	 * @param args       方法参数类型
	 * @return 方法调用结果
	 */
	private Object invoke(final Object target, final String methodName, final Class<?>... args) {
		return AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					Method method = method(target, methodName, args);
					method.setAccessible(true);
					return method.invoke(target);
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		});
	}

	/**
	 * 获取对象的指定方法
	 *
	 * @param target     目标对象
	 * @param methodName 方法名
	 * @param args       方法参数类型
	 * @return 方法对象
	 * @throws NoSuchMethodException 如果方法不存在
	 */
	private Method method(Object target, String methodName, Class<?>[] args)
		throws NoSuchMethodException {
		try {
			return target.getClass().getMethod(methodName, args);
		} catch (NoSuchMethodException e) {
			return target.getClass().getDeclaredMethod(methodName, args);
		}
	}

	/**
	 * 获取ByteBuffer的原始缓冲区
	 * 用于清理直接内存
	 *
	 * @param buffer 字节缓冲区
	 * @return 原始的ByteBuffer对象
	 */
	private ByteBuffer viewed(ByteBuffer buffer) {
		String methodName = "viewedBuffer";
		Method[] methods = buffer.getClass().getMethods();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equals("attachment")) {
				methodName = "attachment";
				break;
			}
		}

		ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
		if (viewedBuffer == null) {
			return buffer;
		} else {
			return viewed(viewedBuffer);
		}
	}

	/**
	 * CommitLog文件路径信息类
	 * 用于封装CommitLog文件的名称和路径信息
	 */
	@Setter
	@Getter
	class CommitLogFilePath {
		/**
		 * 文件名
		 */
		private String fileName;
		/**
		 * 完整文件路径
		 */
		private String filePath;

		public CommitLogFilePath(String fileName, String filePath) {
			this.fileName = fileName;
			this.filePath = filePath;
		}

	}

	/**
	 * 创建新的CommitLog文件
	 * 当当前CommitLog文件写满时，创建新的文件��续写入
	 *
	 * @param topicName      主题名称
	 * @param commitLogModel CommitLog模型
	 * @return 新创建的CommitLog文件路径信息
	 */
	private CommitLogFilePath createNewCommitLogFile(String topicName, CommitLogModel commitLogModel) {
		String newFileName = LogFileNameUtil.incrCommitLogFileName(commitLogModel.getFileName());
		String newFilePath = LogFileNameUtil.buildCommitLogFilePath(topicName, newFileName);
		File newCommitLogFile = new File(newFilePath);
		try {
			//新的commitLog文件创建
			newCommitLogFile.createNewFile();
			log.info("创建了新的commitLog文件");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new CommitLogFilePath(newFileName, newFilePath);
	}
}
