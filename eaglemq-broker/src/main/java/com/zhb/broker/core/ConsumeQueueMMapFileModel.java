package com.zhb.broker.core;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.model.QueueModel;
import com.zhb.broker.utils.LogFileNameUtil;
import com.zhb.broker.utils.PutMessageLock;
import com.zhb.broker.utils.UnfailReentrantLock;
import com.zhb.common.constants.BrokerConstants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 消费队列内存映射文件模型
 * 对消费队列(ConsumeQueue)文件进行内存映射的核心类
 * 提供消费队列文件的读写和管理功能，通过内存映射技术实现高效IO操作
 */
@Slf4j
public class ConsumeQueueMMapFileModel {
	/**
	 * 消费队列文件对象
	 */
	private File file;
	/**
	 * 内存映射缓冲区，用于写操作
	 */
	private MappedByteBuffer mappedByteBuffer;
	/**
	 * 读操作使用的字节缓冲区
	 */
	private ByteBuffer readBuffer;
	/**
	 * 文件通道，用于文件操作和内存映射
	 */
	private FileChannel fileChannel;
	/**
	 * 当前消费队列对应的主题
	 */
	private String topic;
	/**
	 * 当前消费队列的队列ID
	 * -- GETTER --
	 * 获取当��队列ID
	 *
	 * @return 队列ID
	 */
	@Getter
	private Integer queueId;
	/**
	 * 消费队列文件名
	 */
	private String consumeQueueFileName;
	/**
	 * 写入消息的锁，保证线程安全
	 */
	private PutMessageLock putMessageLock;


	/**
	 * 加载文件到内存映射
	 * 将指定主题和队列ID的消费队列文件映射到内存中
	 *
	 * @param topicName         消息主题名称
	 * @param queueId           队列ID
	 * @param startOffset       开始映射的偏移量位置
	 * @param latestWriteOffset 最新写入的偏移量位置
	 * @param mappedSize        映射的大小(字节)
	 * @throws IOException 如果文件操作失败
	 */
	public void loadFileInMMap(String topicName, Integer queueId, int startOffset, int latestWriteOffset, int mappedSize) throws IOException {
		this.topic = topicName;
		this.queueId = queueId;
		String filePath = getLatestConsumeQueueFile();
		this.doMMap(filePath, startOffset, latestWriteOffset, mappedSize);
		//默认非公平
		putMessageLock = new UnfailReentrantLock();
	}

	/**
	 * 写入内容并可选择是否强制刷盘
	 * 将数据写入到内存映射的消费队列文件中
	 *
	 * @param content 要写入的字节数组内容
	 * @param force   是否强制刷盘，true立即写入磁盘，false由操作系统决定
	 */
	public void writeContent(byte[] content, boolean force) {
		try {
			putMessageLock.lock();
			mappedByteBuffer.put(content);
			if (force) {
				mappedByteBuffer.force();
			}
		} finally {
			putMessageLock.unlock();
		}
	}

	/**
	 * 写入内容（默认不强制刷盘）
	 * 调用重载的writeContent方法，默认不进行强制刷盘操作
	 *
	 * @param content 要写入的字节数组内容
	 */
	public void writeContent(byte[] content) {
		writeContent(content, false);
	}


	/**
	 * 读取单条消费队列数据
	 * 从指定偏移量位置读取一条消费队列数据（固定16字节大小）
	 *
	 * @param pos 读取开始位置的偏移量
	 * @return 读取到的字节数组内容
	 */
	public byte[] readContent(int pos) {
		//ConsumeQueue每个单元文件存储的固定大小是12字节
		//readBuffer pos:0~limit(开了一个窗口) -》readBuf
		//readBuf 任意修改
		ByteBuffer readBuf = readBuffer.slice();
		readBuf.position(pos);
		byte[] content = new byte[BrokerConstants.CONSUME_QUEUE_EACH_MSG_SIZE];
		readBuf.get(content);
		return content;
	}


	/**
	 * 批量读取消费队列数据
	 * 从指定偏移量位置开始读取多条消费队列数据
	 *
	 * @param pos      读取开始位置的偏移量
	 * @param msgCount 要读取的消息条数
	 * @return 包含多条消息数据的字节数组列表
	 */
	public List<byte[]> readContent(int pos, int msgCount) {
		ByteBuffer readBuf = readBuffer.slice();
		readBuf.position(pos);
		List<byte[]> loadContentList = new ArrayList<>();
		for (int i = 0; i < msgCount; i++) {
			byte[] content = new byte[BrokerConstants.CONSUME_QUEUE_EACH_MSG_SIZE];
			readBuf.get(content);
			loadContentList.add(content);
		}
		return loadContentList;
	}


	/**
	 * 执行内存映射操作
	 * 将文件映射到内存中，便于高效读写
	 *
	 * @param filePath          文件路径
	 * @param startOffset       开始映射的偏移量
	 * @param latestWriteOffset 最新写入的偏移量
	 * @param mappedSize        映射的大小
	 * @throws IOException 如果文件操作失败
	 */
	private void doMMap(String filePath, int startOffset, int latestWriteOffset, int mappedSize) throws IOException {
		file = new File(filePath);
		if (!file.exists()) {
			throw new FileNotFoundException("filePath is " + filePath + " inValid");
		}
		this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
		this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
		this.readBuffer = mappedByteBuffer.slice();
		this.mappedByteBuffer.position(latestWriteOffset);
	}

	/**
	 * 获取最新的消费队列文件路径
	 * 检查当前队列模型并返回可用的消费队列文件路径
	 *
	 * @return 消费队列文件的完整路径
	 */
	private String getLatestConsumeQueueFile() {
		EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(topic);
		if (eagleMqTopicModel == null) {
			throw new IllegalArgumentException("topic is inValid! topicName is " + topic);
		}
		List<QueueModel> queueModelList = eagleMqTopicModel.getQueueList();
		QueueModel queueModel = queueModelList.get(queueId);
		if (queueModel == null) {
			throw new IllegalArgumentException("queueId is inValid! queueId is " + queueId);
		}
		int diff = queueModel.getOffsetLimit();
		String filePath = null;
		if (diff == 0) {
			//已经写满了 需要创建新文件
			filePath = this.createNewConsumeQueueFile(queueModel.getFileName());
		} else if (diff > 0) {
			//还有空间可写入
			filePath = LogFileNameUtil.buildConsumeQueueFilePath(topic, queueId, queueModel.getFileName());
		}
		return filePath;
	}

	/**
	 * 创建新的消费队列文件
	 * 当当前文件写满时，创建新的文件继续写��
	 *
	 * @param fileName 当前文件名
	 * @return 新创建的消费队列文件路径
	 */
	private String createNewConsumeQueueFile(String fileName) {
		String newFileName = LogFileNameUtil.incrConsumeQueueFileName(fileName);
		String newFilePath = LogFileNameUtil.buildConsumeQueueFilePath(topic, queueId, newFileName);
		File newConsumeQueueFile = new File(newFilePath);
		try {
			//新的消费队列文件创建
			newConsumeQueueFile.createNewFile();
			log.info("创建了新的consumeQueue文件");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return newFilePath;
	}
}
