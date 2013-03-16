package com.ebay.kvstore.server.data.cache;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RandomCacheReplacer extends BaseCacheReplacer {

	protected Set<byte[]> index;

	public RandomCacheReplacer() {
		index = new HashSet<>();
	}

	@Override
	public byte[] getReplacement() {
		byte[] result = null;
		Iterator<byte[]> it = index.iterator();
		if (it.hasNext()) {
			result = it.next();
			it.remove();
		}
		return result;
	}

	@Override
	public void reIndex(byte[] key) {

	}

	@Override
	public void deleteIndex(byte[] key) {
		index.remove(key);
	}

	@Override
	public void addIndex(byte[] key) {
		index.add(key);
	}

}
