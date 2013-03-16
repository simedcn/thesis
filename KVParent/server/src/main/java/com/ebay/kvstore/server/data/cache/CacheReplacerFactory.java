package com.ebay.kvstore.server.data.cache;

public class CacheReplacerFactory {
	/**
	 * 
	 * @param name
	 *            (lru, fifo, random) default is random
	 * @return
	 */
	public static ICacheReplacer createReplacer(String name) {
		switch (name) {
		case ICacheReplacer.FIFO:
			return new FIFOCacheReplacer();
		case ICacheReplacer.LRU:
			return new LRUCacheReplacer();
		case ICacheReplacer.RANDOM:
		default:
			return new RandomCacheReplacer();
		}
	}

}
