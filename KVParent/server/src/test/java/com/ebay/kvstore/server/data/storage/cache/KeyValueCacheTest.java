package com.ebay.kvstore.server.data.storage.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.server.data.cache.FIFOCacheReplacer;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.structure.Value;
import com.ebay.kvstore.util.KeyValueUtil;

public class KeyValueCacheTest {

	private KeyValueCache unlimitCache;

	private KeyValueCache limitCache;

	@Before
	public void initTest() {
		unlimitCache = KeyValueCache.forBuffer();
		limitCache = KeyValueCache.forCache(32, new FIFOCacheReplacer());
	}

	@Test
	public void testUsed() {
		unlimitCache.set(new byte[] { 1 }, new Value(new byte[] { 1, 2 }));
		unlimitCache.set(new byte[] { 1, 2 }, new Value(new byte[] { 1, 2 }));
		unlimitCache.set(new byte[] { 1 }, new Value(new byte[] { 1, 2, 3 }));
		assertEquals(40, unlimitCache.getUsed());

		unlimitCache.delete(new byte[] { 1 });
		assertEquals(20, unlimitCache.getUsed());

	}

	@Test
	public void testOperation() {
		byte[] key1 = new byte[] { 1 };
		byte[] key2 = new byte[] { 2 };
		byte[] key3 = new byte[] { 3 };

		unlimitCache.set(key1, new Value(new byte[] { 1, 2 }));
		assertArrayEquals(new byte[] { 1, 2 }, unlimitCache.get(key1).getValue().getValue());
		assertNull(unlimitCache.get(key2));

		unlimitCache.delete(key1);
		assertNull(unlimitCache.get(key1));

	}

	@Test
	public void testReplacement() {
		for (int i = 0; i < 128; i++) {
			limitCache.set(new byte[] { (byte) i }, new Value(new byte[] { (byte) i }));
			assertTrue(limitCache.getUsed() <= 32);
		}
	}

}
