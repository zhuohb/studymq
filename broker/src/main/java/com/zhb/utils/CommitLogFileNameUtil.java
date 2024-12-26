package com.zhb.utils;

public class CommitLogFileNameUtil {
	/**
	 * 构建第一份commitLog文件名称
	 *
	 * @return
	 */
	public static String buildFirstCommitLogName() {
		return "00000000";
	}

	/**
	 * 根据老的commitLog文件名生成新的commitLog文件名
	 *
	 * @param oldFileName
	 * @return
	 */
	public static String incrCommitLogFileName(String oldFileName) {
		if (oldFileName.length() != 8) {
			throw new IllegalArgumentException("fileName must has 8 chars");
		}
		//转换为long类型,00000012 -> 12
		Long fileIndex = Long.valueOf(oldFileName);
		fileIndex++;
		String newFileName = String.valueOf(fileIndex);
		int newFileNameLen = newFileName.length();
		int needFullLen = 8 - newFileNameLen;
		if (needFullLen < 0) {
			throw new RuntimeException("unKnow fileName error");
		}
		//补位
		StringBuilder stb = new StringBuilder();
		for (int i = 0; i < needFullLen; i++) {
			stb.append("0");
		}
		stb.append(newFileName);
		return stb.toString();
	}

	public static void main(String[] args) {
		String newFileName = CommitLogFileNameUtil.incrCommitLogFileName("00000011");
		System.out.println(newFileName);
	}
}
