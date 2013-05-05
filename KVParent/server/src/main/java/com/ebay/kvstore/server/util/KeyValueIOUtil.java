package com.ebay.kvstore.server.util;

import java.io.IOException;

import com.ebay.kvstore.server.data.storage.fs.IBlockInputStream;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

public class KeyValueIOUtil {

	public static KeyValue readFromExternal(IBlockInputStream in) throws IOException {
		// used for skipping key/values quickly
		int len = in.readInt();
		int keyLen = in.readInt();
		byte[] key = new byte[keyLen];
		in.readFully(key, 0, keyLen);
		byte[] value = new byte[len - keyLen - 12];
		in.readFully(value, 0, value.length);
		long expire = in.readLong();
		KeyValue kv = new KeyValue(key, new Value(value, expire));
		return kv;
	}

	public static void writeToExternal(IBlockOutputStream out, KeyValue kv) throws IOException {
		if (kv == null) {
			return;
		}
		int len = kv.getKey().length + kv.getValue().getValue().length + 12;
		out.writeInt(len);
		out.writeInt(kv.getKey().length);
		out.write(kv.getKey());
		out.write(kv.getValue().getValue());
		out.writeLong(kv.getValue().getExpire());
	}

}
