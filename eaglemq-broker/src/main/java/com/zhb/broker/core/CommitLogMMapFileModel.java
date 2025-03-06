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
 * @Author idea
 * @Date: Created in 22:50 2024/3/25
 * @Description 最基础的mmap对象模型
 */
@Slf4j
public class CommitLogMMapFileModel {


	private File file;
	private MappedByteBuffer mappedByteBuffer;
	private ByteBuffer readByteBuffer;
	private FileChannel fileChannel;
	private String topic;
	private PutMessageLock putMessageLock;


	/**
	 * 指定offset做文件的映射
	 *
	 * @param topicName   消息主题
	 * @param startOffset 开始映射的offset
	 * @param mappedSize  映射的体积 (byte)
	 */
	public void loadFileInMMap(String topicName, int startOffset, int mappedSize) throws IOException {
		this.topic = topicName;
		String filePath = getLatestCommitLogFile(topicName);
		this.doMMap(filePath, startOffset, mappedSize);
		//默认非公平
		putMessageLock = new UnfailReentrantLock();
	}

	/**
	 * 执行mmap步骤
	 *
	 * @param filePath
	 * @param startOffset
	 * @param mappedSize
	 * @throws IOException
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
	 * 获取最新的commitLog文件路径
	 *
	 * @param topicName
	 * @return
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
			//已经写满了
			CommitLogFilePath commitLogFilePath = this.createNewCommitLogFile(topicName, commitLogModel);
			filePath = commitLogFilePath.getFilePath();
		} else if (diff > 0) {
			//还有机会写入
			filePath = LogFileNameUtil.buildCommitLogFilePath(topicName, commitLogModel.getFileName());
		}
		return filePath;
	}


	/**
	 * 更高性能的一种写入api
	 *
	 * @param messageDTO
	 */
	public void writeContent(MessageDTO messageDTO) throws IOException {
		this.writeContent(messageDTO, false);
	}

	/**
	 * 写入数据到磁盘当中
	 *
	 * @param messageDTO
	 */
	public void writeContent(MessageDTO messageDTO, boolean force) throws IOException {
		//定位到最新的commitLog文件中，记录下当前文件是否已经写满，如果写满，则创建新的文件，并且做新的mmap映射 done
		//如果当前文件没有写满，对content内容做一层封装 done
		//再判断写入是否会导致commitLog写满，如果不会，则选择当前commitLog，如果会则创建新文件，并且做mmap映射 done
		//定位到最新的commitLog文件之后，写入 done

		//定义一个对象专门管理各个topic的最新写入offset值，并且定时刷新到磁盘中（缺少了同步到磁盘的机制）
		//写入数据，offset变更，如果是高并发场景，offset是不是会被多个线程访问？

		//offset会用一个原子类AtomicLong去管理
		//线程安全问题：线程1：111，线程2：122
		//加锁机制 （锁的选择非常重要）
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
			//强制刷盘
			mappedByteBuffer.force();
		}
		putMessageLock.unlock();
	}

	/**
	 * 支持从文件的指定offset开始读取内容
	 *
	 * @param pos
	 * @param length
	 * @return
	 */
//    public byte[] readContent(int pos, int length) {
//        ByteBuffer readBuf = readByteBuffer.slice();
//        readBuf.position(pos);
//        byte[] readBytes = new byte[length];
//        readBuf.get(readBytes);
//        return readBytes;
//    }
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
	 * 将ConsumerQueue文件写入
	 *
	 * @param messageDTO
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
		//刷新offset
		QueueModel queueModel = eagleMqTopicModel.getQueueList().get(queueId);
		queueModel.getLatestOffset().addAndGet(content.length);
	}

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
	 * 释放mmap内存占用
	 */
	public void clean() {
		if (mappedByteBuffer == null || !mappedByteBuffer.isDirect() || mappedByteBuffer.capacity() == 0) {
			return;
		}
		invoke(invoke(viewed(mappedByteBuffer), "cleaner"), "clean");
	}

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

	private Method method(Object target, String methodName, Class<?>[] args)
		throws NoSuchMethodException {
		try {
			return target.getClass().getMethod(methodName, args);
		} catch (NoSuchMethodException e) {
			return target.getClass().getDeclaredMethod(methodName, args);
		}
	}

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


	class CommitLogFilePath {
		private String fileName;
		private String filePath;

		public CommitLogFilePath(String fileName, String filePath) {
			this.fileName = fileName;
			this.filePath = filePath;
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}
	}

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
