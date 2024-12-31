package com.zhb.model;


import com.zhb.utils.ByteConvertUtils;
import lombok.Data;

/**
 * commitLog真实数据存储对象模型
 */
@Data
public class CommitLogMessageModel {

    /**
     * 消息的体积大小，单位是字节
     */
    private int size;

    /**
     * 真正的消息内容
     */
    private byte[] content;

    public byte[] convertToBytes() {
        byte[] sizeByte = ByteConvertUtils.intToBytes(this.getSize());
        byte[] content = this.getContent();
        byte[] mergeResultByte = new byte[sizeByte.length + content.length];
        int j = 0;
        for (int i = 0; i < sizeByte.length; i++, j++) {
            mergeResultByte[j] = sizeByte[i];
        }
        for (int i = 0; i < content.length; i++, j++) {
            mergeResultByte[j] = content[i];
        }
        return mergeResultByte;
    }
}
