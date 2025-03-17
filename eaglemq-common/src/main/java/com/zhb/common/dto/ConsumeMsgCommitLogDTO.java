package com.zhb.common.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ConsumeMsgCommitLogDTO {

    private String fileName;

    private long commitLogOffset;

    private int commitLogSize;

    private byte[] body;

    private int retryTimes;

}
