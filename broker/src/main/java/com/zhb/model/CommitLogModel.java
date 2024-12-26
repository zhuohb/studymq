package com.zhb.model;

import lombok.Data;

/**
 * commitLog文件的写入offset封装
 */
@Data
public class CommitLogModel {

	/**
	 * 最新commitLog文件的名称
	 */
	private String fileName;

	/**
	 * commitLog文件写入的最大上限体积
	 */
	private Long offsetLimit;
	/**
	 * 最新commitLog文件写入数据的地址
	 */
	private Long offset;
}