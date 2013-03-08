package com.ebay.chluo.kvstore;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

	@Override
	public int compare(byte[] key1, byte[] key2) {
		return KeyValueUtil.compare(key1, key2);
	}

}
