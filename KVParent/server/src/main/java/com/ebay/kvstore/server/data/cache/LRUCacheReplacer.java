package com.ebay.kvstore.server.data.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCacheReplacer extends BaseCacheReplacer {

	protected Map<byte[],Object> index;
	protected Object dummy = new Object();
	
	public LRUCacheReplacer() {
		index = new LinkedHashMap<byte[],Object>(16, .75f, true);
	}

	@Override
	public void addIndex(byte[] key, long expire) {
		index.put(key,dummy);
	}

	@Override
	public void deleteIndex(byte[] key) {
		index.remove(key);
	}

	@Override
	public byte[] getReplacement() {
		Iterator<byte[]> it = index.keySet().iterator();
		byte[] replacement = null;
		if (it.hasNext()) {
			replacement = it.next();
			index.remove(replacement);
		}
		return replacement;
	}

	@Override
	public void reIndex(byte[] key) {
		index.get(key);
	}

}
