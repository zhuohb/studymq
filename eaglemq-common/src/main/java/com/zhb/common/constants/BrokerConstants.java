package com.zhb.common.constants;


public class BrokerConstants {

	public static final String EAGLE_MQ_HOME = "EAGLE_MQ_HOME";
	public static final String BASE_COMMIT_PATH = "/commitlog/";
	public static final String BASE_CONSUME_QUEUE_PATH = "/consumequeue/";
	public static final String BROKER_PROPERTIES_PATH = "/config/broker.properties";
	public static final String SPLIT = "/";
	public static final Integer COMMIT_LOG_DEFAULT_MMAP_SIZE = 1 * 1024 * 1024; //1mb单位，方便讲解使用
	public static final Integer COMSUMEQUEUE_DEFAULT_MMAP_SIZE = 1 * 1024 * 1024; //1mb单位，方便讲解使用
	public static final Integer DEFAULT_REFRESH_MQ_TOPIC_TIME_STEP = 3;
	public static final Integer DEFAULT_REFRESH_CONSUME_QUEUE_OFFSET_TIME_STEP = 1;
	public static final int CONSUME_QUEUE_EACH_MSG_SIZE = 16;
	public static final short DEFAULT_MAGIC_NUM = 17671;
}
