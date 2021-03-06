package com.ebay.kvstore.server.data.storage.cache;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.server.data.cache.FIFOCacheReplacer;
import com.ebay.kvstore.server.data.cache.ICacheReplacer;
import com.ebay.kvstore.server.data.cache.LRUCacheReplacer;

public class CacheReplacerTest {

	private ICacheReplacer lru;

	private ICacheReplacer fifo;

	@Before
	public void init() {
		lru = new LRUCacheReplacer();
		fifo = new FIFOCacheReplacer();
	}

	@Test
	public void test() {
		byte[] key1 = new byte[] { 1 };
		byte[] key2 = new byte[] { 2 };
		byte[] key3 = new byte[] { 3 };
		byte[] key4 = new byte[] { 4 };
		lru.addIndex(key1, 0);
		lru.addIndex(key2, 0);
		lru.addIndex(key3, 0);
		lru.addIndex(key4, 0);
		lru.reIndex(key1);
		assertArrayEquals(key2, lru.getReplacement());
		lru.deleteIndex(key3);
		assertArrayEquals(key4, lru.getReplacement());

		fifo.addIndex(key1, 0);
		fifo.addIndex(key2, 0);
		fifo.addIndex(key3, 0);
		fifo.addIndex(key4, 0);
		fifo.deleteIndex(key3);
		assertArrayEquals(key1, lru.getReplacement());

	}

}
