package com.zhb.broker.model;

/**
 * @Author idea
 * @Date: Created in 10:24 2024/3/30
 * @Description commitLog真实数据存储对象模型
 */
public class CommitLogMessageModel {

    /**
     * 真正的消息内容
     */
    private byte[] content;

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public byte[] convertToBytes() {
//        byte[] sizeByte = ByteConvertUtils.intToBytes(this.getSize());
//        byte[] content = this.getContent();
//        byte[] mergeResultByte = new byte[sizeByte.length + content.length];
//        int j=0;
//        for (int i = 0; i < sizeByte.length; i++,j++) {
//            mergeResultByte[j] = sizeByte[i];
//        }
//        for (int i = 0; i < content.length; i++,j++) {
//            mergeResultByte[j] = content[i];
//        }
//        return mergeResultByte;
        return content;
    }
}
