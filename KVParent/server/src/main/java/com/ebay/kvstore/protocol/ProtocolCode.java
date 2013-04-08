package com.ebay.kvstore.protocol;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.ServerConstants;
import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;

/**
 * Defines protocol return code and corresponding message
 * 
 * @author luochen
 * 
 */
public class ProtocolCode {

	private static Logger logger = LoggerFactory.getLogger(ProtocolCode.class);

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
		loadMessage(ServerConstants.Message_Conf_Path);
	}

	public static String getMessage(int code) {
		return messages.get(code);
	}

	private static void loadMessage(String path) {
		messages = new HashMap<>();
		messages.put(Success, "success");
		messages.put(Invalid_Key, "the given key is not hosted in the data server");
		messages.put(Dataserver_Io_Error, "fail to fetch data, data server internal error occurs");
		messages.put(Invalid_Region, "the given region is not hosted in the data server");
		messages.put(Master_Error, "error occured in master server");
		messages.put(Duplicate_Dataserver_Error,
				"the data server with same address already exists in cluster");
		messages.put(Invalid_Counter, "the given key does not corespond to a counter");
		try {
			IConfiguration conf = ConfigurationLoader.load(path);
			if (conf != null) {
				for (Entry<Object, Object> e : conf) {
					String key = (String) e.getKey();
					String message = (String) e.getValue();
					try {
						int field = getField(key);
						messages.put(field, message);
					} catch (ReflectiveOperationException re) {
						logger.error("Invalid message configuration {}:{}", key, message);
					}
				}
			}
		} catch (IOException e) {
			logger.error("Error loading message configuration", e);
		}

	}

	private static int getField(String key) throws ReflectiveOperationException {
		StringBuilder sb = new StringBuilder(key.length());
		boolean split = true;
		for (int i = 0; i < key.length(); i++) {
			char ch = key.charAt(i);
			if (ch == '.') {
				split = true;
				sb.append('_');
			} else {
				if (split) {
					ch = Character.toUpperCase(ch);
					split = false;
				}
				sb.append(ch);
			}
		}
		Field field = ProtocolCode.class.getField(sb.toString());
		return field.getInt(null);
	}

}
