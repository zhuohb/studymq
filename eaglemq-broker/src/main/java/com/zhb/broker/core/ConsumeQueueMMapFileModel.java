package com.zhb.broker.core;

import com.zhb.broker.cache.CommonCache;
import com.zhb.broker.model.EagleMqTopicModel;
import com.zhb.broker.model.QueueModel;
import com.zhb.broker.utils.LogFileNameUtil;
import com.zhb.broker.utils.PutMessageLock;
import com.zhb.broker.utils.UnfailReentrantLock;
import com.zhb.common.constants.BrokerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @Author idea
 * @Date: Created in 10:33 2024/4/21
 * @Description 对consumeQueue文件做mmap映射的核心对象
 */
public class ConsumeQueueMMapFileModel {

    private final Logger logger = LoggerFactory.getLogger(ConsumeQueueMMapFileModel.class);

    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private ByteBuffer readBuffer;
    private FileChannel fileChannel;
    private String topic;
    private Integer queueId;
    private String consumeQueueFileName;
    private PutMessageLock putMessageLock;


    /**
     * 指定offset做文件的映射
     *
     * @param topicName         消息主题
     * @param queueId           队列id
     * @param startOffset       开始映射的offset
     * @param latestWriteOffset 最新写入的offset
     * @param mappedSize        映射的体积 (byte)
     */
    public void loadFileInMMap(String topicName, Integer queueId, int startOffset, int latestWriteOffset, int mappedSize) throws IOException {
        this.topic = topicName;
        this.queueId = queueId;
        String filePath = getLatestConsumeQueueFile();
        this.doMMap(filePath, startOffset, latestWriteOffset, mappedSize);
        //默认非公平
        putMessageLock = new UnfailReentrantLock();
    }

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

    public void writeContent(byte[] content) {
        writeContent(content, false);
    }


    /**
     * 读取consumequeue数据内容
     *
     * @param pos
     * @return
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
     * 读取consumequeue数据内容
     *
     * @param pos      消息读取开始位置
     * @param msgCount 消息条数
     * @return
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
     * 执行mmap步骤
     *
     * @param filePath
     * @param startOffset
     * @param mappedSize
     * @throws IOException
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
     * 获取最新的commitLog文件路径
     *
     * @return
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
            //已经写满了
            filePath = this.createNewConsumeQueueFile(queueModel.getFileName());
        } else if (diff > 0) {
            //还有机会写入
            filePath = LogFileNameUtil.buildConsumeQueueFilePath(topic, queueId, queueModel.getFileName());
        }
        return filePath;
    }

    private String createNewConsumeQueueFile(String fileName) {
        String newFileName = LogFileNameUtil.incrConsumeQueueFileName(fileName);
        String newFilePath = LogFileNameUtil.buildConsumeQueueFilePath(topic, queueId, newFileName);
        File newConsumeQueueFile = new File(newFilePath);
        try {
            //新的commitLog文件创建
            newConsumeQueueFile.createNewFile();
            logger.info("创建了新的consumeQueue文件");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return newFilePath;
    }

    public Integer getQueueId() {
        return queueId;
    }
}
