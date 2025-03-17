package com.zhb.common.utils;

import java.util.List;

/**
 * 断言工具
 */
public class AssertUtils {


	public static void isNotBlank(String val, String msg) {
		if (val == null || val.trim().isEmpty()) {
			throw new RuntimeException(msg);
		}
	}

	public static void isNotEmpty(List list, String msg) {
		if (list == null || list.isEmpty()) {
			throw new RuntimeException(msg);
		}
	}


	public static void isNotNull(Object val, String msg) {
		if (val == null) {
			throw new RuntimeException(msg);
		}
	}

	public static void isTrue(Boolean condition, String msg) {
		if (!condition) {
			throw new RuntimeException(msg);
		}
	}
}
