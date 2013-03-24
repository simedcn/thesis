package com.ebay.kvstore.protocol;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines protocol return code and corresponding message
 * 
 * @author luochen
 * 
 */
public class ProtocolCode {

	public static final int Success = 0;

	public static final int InvalidKey = 1;

	public static final int IOError = 2;

	public static final int InvalidRegion = 3;

	public static final int Master_Error = 4;

	public static final int DataServer_Exists = 5;

	private static Map<Integer, String> messages;

	static {
		messages = new HashMap<>();
		messages.put(Success, "success");
		messages.put(InvalidKey, "the given key is not hosted in this data server");
		messages.put(IOError, "fail to fetch key, data server internal error occurs");
		messages.put(InvalidRegion, "the given region is not hosted in this data server");
		messages.put(Master_Error, "error occured in master server");
		messages.put(DataServer_Exists,
				"the data server with same address already exists in cluster");
	}

	public static String getMessage(int code) {
		return messages.get(code);
	}

}
