package com.ebay.kvstore.util;

import java.util.Arrays;

import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

public class KeyValueUtil {
	final static long LONG_MASK = 0xffffffffL;

	public static int bytesToInt(byte[] bs) {
		if (bs.length < 4) {
			throw new IllegalArgumentException(
					"The length of the given bytes is less than 4, input:" + bs);
		}
		int targets = (bs[0] & 0xff) | ((bs[1] << 8) & 0xff00) | ((bs[2] << 24) >>> 8)
				| (bs[3] << 24);
		return targets;
	}

	/**
	 * compare the given two keys A little bit tricky for comparison between
	 * byte&&LONG_MASK, 1<10, and -1>10, while 1<-1
	 * 
	 * @param key1
	 * @param key2
	 * @return
	 */
	public static int compare(byte[] key1, byte[] key2) {
		if (key1 == null || key2 == null) {
			// null key is the max
			if (key1 != null) {
				return -1;
			} else if (key2 != null) {
				return 1;
			} else {
				return 0;
			}
		}
		int len1 = key1.length;
		int len2 = key2.length;
		if (len1 < len2)
			return -1;
		if (len1 > len2)
			return 1;
		for (int i = 0; i < len1; i++) {
			byte a = key1[i];
			byte b = key2[i];
			if (a != b)
				return ((a & LONG_MASK) < (b & LONG_MASK)) ? -1 : 1;
		}
		return 0;
	}

	public static boolean equals(byte[] key1, byte[] key2) {
		return compare(key1, key2) == 0;
	}

	public static long getExpireTime(int ttl) {
		if (ttl <= 0) {
			return 0;
		}
		long time = System.currentTimeMillis() + ttl;
		return time;
	}

	public static int getKeyValueLen(byte[] key, Value value) {
		int valueLen = 0;
		if (value == null || value.getValue() == null) {
			valueLen = 0;
		} else {
			valueLen = value.getValue().length + 8;
		}
		return 8 + key.length + valueLen;
	}

	public static int getKeyValueLen(KeyValue kv) {
		return 8 + kv.getKey().length + kv.getValue().getValue().length + 8;
	}

	public static int getTtl(long expire) {
		if (expire <= 0) {
			return 0;
		}
		long time = expire - System.currentTimeMillis();
		return (int) time;
	}

	public static byte[] getValue(KeyValue kv) {
		if (kv != null && kv.getValue() != null) {
			return kv.getValue().getValue();
		}
		return null;
	}

	public static boolean greater(byte[] key1, byte[] key2) {
		return compare(key1, key2) > 0;
	}

	public static boolean inRange(byte[] key, byte[] start, byte[] end) {
		int e1 = KeyValueUtil.compare(key, start);
		int e2 = KeyValueUtil.compare(key, end);
		if (e1 >= 0 && e2 <= 0) {
			return true;
		} else {
			return false;
		}
	}

	public static byte[] intToBytes(int i) {
		byte[] targets = new byte[4];
		targets[0] = (byte) (i & 0xff);
		targets[1] = (byte) ((i >> 8) & 0xff);
		targets[2] = (byte) ((i >> 16) & 0xff);
		targets[3] = (byte) (i >>> 24);
		return targets;
	}

	public static boolean isAlive(long expire) {
		if (expire <= 0) {
			return true;
		}
		long time = System.currentTimeMillis();
		return time <= expire;
	}

	public static boolean isAlive(Value value) {
		if (value == null) {
			return false;
		}
		long expire = value.getExpire();
		return isAlive(expire);
	}

	public static boolean less(byte[] key1, byte[] key2) {
		return compare(key1, key2) < 0;
	}

	public static byte[] nextKey(byte[] key) {
		if (key == null) {
			return null;
		}
		int index = key.length - 1;
		byte[] nextKey = null;
		while (key[index] == 0xff) {
			index--;
		}
		if (index == -1) {
			nextKey = new byte[key.length + 1];
			Arrays.fill(nextKey, (byte) 0);
			nextKey[0] = 1;
		} else {
			nextKey = new byte[key.length];
			for (int i = 0; i < nextKey.length; i++) {
				if (i < index) {
					nextKey[i] = key[i];
				} else if (i == index) {
					nextKey[i] = (byte) (key[i] + 1);
				} else {
					nextKey[i] = 0;
				}
			}
		}
		return nextKey;
	}
}
