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

	public static final int Invalid_Key = 1;

	public static final int Dataserver_Io_Error = 2;

	public static final int Invalid_Region = 3;

	public static final int Master_Error = 4;

	public static final int Duplicate_Dataserver_Error = 5;

	public static final int Invalid_Counter = 6;

	private static Map<Integer, String> messages;

	static {
		messages = new HashMap<>();
		loadMessage();
	}

	public static String getMessage(int code) {
		return messages.get(code);
	}

	private static void loadMessage() {
		messages = new HashMap<>();
		messages.put(Success, "success");
		messages.put(Invalid_Key, "the given key is not hosted in the data server");
		messages.put(Dataserver_Io_Error, "fail to fetch data, data server internal error occurs");
		messages.put(Invalid_Region, "the given region is not hosted in the data server");
		messages.put(Master_Error, "error occured in master server");
		messages.put(Duplicate_Dataserver_Error,
				"the data server with same address already exists in cluster");
		messages.put(Invalid_Counter, "the given key does not corespond to a counter");

	}

}
