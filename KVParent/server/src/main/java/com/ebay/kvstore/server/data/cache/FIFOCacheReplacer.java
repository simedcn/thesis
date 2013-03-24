package com.ebay.kvstore.server.data.cache;

import java.util.LinkedList;
import java.util.List;

public class FIFOCacheReplacer extends BaseCacheReplacer {

	protected List<byte[]> index;

	public FIFOCacheReplacer() {
		index = new LinkedList<>();
	}

	@Override
	public void addIndex(byte[] key) {
		index.add(key);
	}

	@Override
	public void deleteIndex(byte[] key) {
		// TODO Auto-generated method stub
		index.remove(key);
	}

	@Override
	public byte[] getReplacement() {
		byte[] result = null;
		if (index.size() > 0) {
			result = index.remove(0);
		}
		return result;
	}

	@Override
	public void reIndex(byte[] key) {

	}

}
