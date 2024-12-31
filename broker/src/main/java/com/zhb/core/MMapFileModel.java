package com.zhb.core;


import com.zhb.cache.CommonCache;
import com.zhb.constants.BrokerConstants;
import com.zhb.model.CommitLogMessageModel;
import com.zhb.model.CommitLogModel;
import com.zhb.model.EagleMqTopicModel;
import com.zhb.utils.CommitLogFileNameUtil;
import com.zhb.utils.PutMessageLock;
import com.zhb.utils.UnfailReentrantLock;

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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 最基础的mmap对象模型
 */
public class MMapFileModel {

    private File file;
    private MappedByteBuffer mappedByteBuffer;
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
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
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
            filePath = CommitLogFileNameUtil.buildCommitLogFilePath(topicName, commitLogModel.getFileName());
        }
        return filePath;
    }


    /**
     * 支持从文件的指定offset开始读取内容
     *
     * @param readOffset
     * @param size
     * @return
     */
    public byte[] readContent(int readOffset, int size) {
        mappedByteBuffer.position(readOffset);
        byte[] content = new byte[size];
        int j = 0;
        for (int i = 0; i < size; i++) {
            //这里是从内存空间读取数据
            byte b = mappedByteBuffer.get(readOffset + i);
            content[j++] = b;
        }
        return content;
    }


    /**
     * 更高性能的一种写入api
     *
     * @param commitLogMessageModel
     */
    public void writeContent(CommitLogMessageModel commitLogMessageModel) throws IOException {
        this.writeContent(commitLogMessageModel, false);
    }

    /**
     * 写入数据到磁盘当中
     *
     * @param commitLogMessageModel
     */
    public void writeContent(CommitLogMessageModel commitLogMessageModel, boolean force) throws IOException {
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
        this.checkCommitLogHasEnableSpace(commitLogMessageModel);

        //默认刷到page cache中，
        //如果需要强制刷盘，这里要兼容
        putMessageLock.lock();
        mappedByteBuffer.put(commitLogMessageModel.convertToBytes());
        commitLogModel.getOffset().addAndGet(commitLogMessageModel.getSize());
        if (force) {
            //强制刷盘
            mappedByteBuffer.force();
        }
        putMessageLock.unlock();
    }

    private void checkCommitLogHasEnableSpace(CommitLogMessageModel commitLogMessageModel) throws IOException {
        EagleMqTopicModel eagleMqTopicModel = CommonCache.getEagleMqTopicModelMap().get(this.topic);
        CommitLogModel commitLogModel = eagleMqTopicModel.getCommitLogModel();
        long writeAbleOffsetNum = commitLogModel.countDiff();
        //空间不足，需要创建新的commitLog文件并且做映射
        if (!(writeAbleOffsetNum >= commitLogMessageModel.getSize())) {
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
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
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
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
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
        String newFileName = CommitLogFileNameUtil.incrCommitLogFileName(commitLogModel.getFileName());
        String newFilePath = CommitLogFileNameUtil.buildCommitLogFilePath(topicName, newFileName);
        File newCommitLogFile = new File(newFilePath);
        try {
            //新的commitLog文件创建
            newCommitLogFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CommitLogFilePath(newFileName, newFilePath);
    }
}
