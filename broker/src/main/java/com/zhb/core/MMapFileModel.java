package com.zhb.core;

import com.zhb.cache.CommonCache;
import com.zhb.constants.BrokerConstants;
import com.zhb.model.CommitLogModel;
import com.zhb.model.TopicModel;
import com.zhb.utils.CommitLogFileNameUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MMapFileModel {
	private File file;
	private MappedByteBuffer mappedByteBuffer;
	private FileChannel fileChannel;
	private String topic;


	/**
	 * 文件的内存映射
	 *
	 * @param topicName   消息主题
	 * @param startOffset 文件开始映射的offset
	 * @param mappedSize  映射的体积
	 * @throws IOException
	 */
	public void loadFileInMMap(String topicName, int startOffset, int mappedSize) throws IOException {
		String filePath = this.getLatestCommitLogFile(topicName);
		file = new File(filePath);
		if (!file.exists()) {
			throw new FileNotFoundException("filePath is " + filePath + " inValid");
		}
		fileChannel = new RandomAccessFile(file, "rw").getChannel();
		mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
	}

	private String getLatestCommitLogFile(String topicName) {
		TopicModel topicModel = CommonCache.getTopicModelMap().get(topicName);
		if (topicModel == null) {
			throw new IllegalArgumentException("topic is inValid! topicName is " + topicName);
		}
		// 获取最新的commitLog文件路径
		CommitLogModel commitLogModel = topicModel.getCommitLogModel();
		long diff = commitLogModel.getOffsetLimit() - commitLogModel.getOffset();
		String filePath = null;
		if (diff == 0) {
			//commitLog写满了
			filePath = this.createNewCommitLogFile(topicName, commitLogModel);
		} else if (diff > 0) {
			//还有空间可以写入
			filePath = CommonCache.getGlobalProperties().getMqHome() +
				BrokerConstants.BASE_STORE_PATH +
				topicName +
				commitLogModel.getFileName();
		}
		return filePath;
	}

	/**
	 * 创建新的commitLog文件
	 *
	 * @param topicName
	 * @param commitLogModel
	 * @return
	 */
	private String createNewCommitLogFile(String topicName, CommitLogModel commitLogModel) {
		String newFileName = CommitLogFileNameUtil.incrCommitLogFileName(commitLogModel.getFileName());
		String newFilePath = CommonCache.getGlobalProperties().getMqHome()
			+ BrokerConstants.BASE_STORE_PATH
			+ topicName
			+ newFileName;
		File newCommitLogFile = new File(newFilePath);
		try {
			//新的commitLog文件创建
			newCommitLogFile.createNewFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return newFilePath;
	}

	/**
	 * 从指定的位置开始读取内容
	 *
	 * @param readOffset
	 * @param size
	 * @return
	 */
	public byte[] readContent(int readOffset, int size) {
		//设置下一个读取或写入操作的起始位置
		mappedByteBuffer.position(readOffset);
		byte[] content = new byte[size];
		// 直接从 mappedByteBuffer 读取 size 个字节到 content 数组中
		mappedByteBuffer.get(content);
		return content;
	}

	/**
	 * 写入数据到磁盘当中
	 * 默认不强制刷盘
	 *
	 * @param content
	 */
	public void writeContent(byte[] content) {
		this.writeContent(content, false);
	}

	/**
	 * 写入数据到磁盘当中
	 *
	 * @param content
	 * @param force
	 */
	public void writeContent(byte[] content, boolean force) {
		//1. 定位到最新的commitLog文件中，记录下当前文件是否已经写满，如果写满，则创建新的文件，并且做新的mmap映射
		//2. 如果当前文件没有写满，对content内容做一层封装，再判断写入是否会导致commitLog写满，如果不会，则选择当前commitLog，如果会则创建新文件，并且做mmap映射
		//3. 定位到最新的commitLog文件之后，写入
		//4. 定义一个对象专门管理各个topic的最新写入offset值，并且定时刷新到磁盘中（mmap？）
		//5. 写入数据，offset变更，如果是高并发场景，offset是不是会被多个线程访问？

		//offset会用一个原子类AtomicLong去管理
		//线程安全问题：线程1：111，线程2：122
		//加锁机制 （锁的选择非常重要）


		//默认刷到page cache中，
		mappedByteBuffer.put(content);
		if (force) {
			//强制刷盘
			mappedByteBuffer.force();
		}
	}

	/**
	 * 清理内存映射
	 *
	 * @throws IOException
	 */
	public void clean() throws IOException {
		// 强制将缓冲区的内容写入到磁盘
		mappedByteBuffer.force();

		mappedByteBuffer.clear();
		fileChannel.close();
	}

}
