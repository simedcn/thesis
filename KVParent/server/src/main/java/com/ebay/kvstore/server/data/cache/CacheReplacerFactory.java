package com.ebay.kvstore.server.data.cache;

import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.InvalidConfException;

public class CacheReplacerFactory {
	/**
	 * 
	 * @param name
	 *            (lru, fifo, random) default is random
	 * @return
	 */
	public static ICacheReplacer createReplacer(String policy) {
		switch (policy) {
		case ICacheReplacer.FIFO:
			return new FIFOCacheReplacer();
		case ICacheReplacer.LRU:
			return new LRUCacheReplacer();
		case ICacheReplacer.RANDOM:
			return new RandomCacheReplacer();
		default:
			throw new InvalidConfException(IConfigurationKey.DataServer_Cache_Replacement_Policy,
					"fifo|lru|random)", policy);
		}
	}
}
