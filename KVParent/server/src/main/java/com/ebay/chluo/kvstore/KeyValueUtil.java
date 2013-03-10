package com.ebay.chluo.kvstore;

import java.io.IOException;

import com.ebay.chluo.kvstore.data.storage.file.KVInputStream;
import com.ebay.chluo.kvstore.data.storage.file.KVOutputStream;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;

public class KeyValueUtil {
	final static long LONG_MASK = 0xffffffffL;

	public static boolean less(byte[] key1, byte[] key2) {
		return compare(key1, key2) < 0;
	}

	public static boolean equals(byte[] key1, byte[] key2) {
		return compare(key1, key2) == 0;
	}

	public static boolean greater(byte[] key1, byte[] key2) {
		return compare(key1, key2) > 0;
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

	public static byte[] intToBytes(int i) {
		byte[] targets = new byte[4];
		targets[0] = (byte) (i & 0xff);
		targets[1] = (byte) ((i >> 8) & 0xff);
		targets[2] = (byte) ((i >> 16) & 0xff);
		targets[3] = (byte) (i >>> 24);
		return targets;
	}

	public static int bytesToInt(byte[] bs) {
		if (bs.length < 4) {
			throw new IllegalArgumentException("The length of the given bytes is less than 4, input:"
					+ bs);
		}
		int targets = (bs[0] & 0xff) | ((bs[1] << 8) & 0xff00) | ((bs[2] << 24) >>> 8)
				| (bs[3] << 24);
		return targets;
	}

	public static KeyValue readFromExternal(KVInputStream in) throws IOException {
		// used for skipping key/values quickly
		int len = in.readInt();
		int keyLen = in.readInt();
		byte[] key = new byte[keyLen];
		in.readFully(key, 0, keyLen);
		byte[] value = new byte[len - keyLen - 4];
		in.readFully(value, 0, value.length);
		KeyValue kv = new KeyValue(key, new Value(value));
		return kv;
	}

	public static void writeToExternal(KVOutputStream out, KeyValue kv) throws IOException {
		int len = kv.getKey().length + kv.getValue().getValue().length + 4;
		out.writeInt(len);
		out.writeInt(kv.getKey().length);
		out.write(kv.getKey());
		out.write(kv.getValue().getValue());
	}
}
