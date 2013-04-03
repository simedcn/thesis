package com.ebay.kvstore.server.data.cache;

import java.util.LinkedList;
import java.util.List;

public class LRUCacheReplacer extends BaseCacheReplacer {

	protected List<byte[]> index;

	public LRUCacheReplacer() {
		index = new LinkedList<>();
	}

	@Override
	public void addIndex(byte[] key) {
		index.add(key);
	}

	@Override
	public void deleteIndex(byte[] key) {
		index.remove(key);
	}

	@Override
	public byte[] getReplacement() {
		if (index.size() > 0) {
			return index.remove(0);
		} else {
			return null;
		}
	}

	@Override
	public void reIndex(byte[] key) {
		index.remove(key);
		index.add(key);
	}

}
