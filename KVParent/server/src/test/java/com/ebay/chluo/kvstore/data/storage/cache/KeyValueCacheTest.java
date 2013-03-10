package com.ebay.chluo.kvstore.data.storage.cache;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;

public class KeyValueCacheTest {

	private KeyValueCache unlimitCache;

	private KeyValueCache limitCache;

	@Before
	public void initTest() {
		unlimitCache = new KeyValueCache(0, null);
		limitCache = new KeyValueCache(32, new FIFOCacheReplacer());

	}

	@Test
	public void testUsed() {
		unlimitCache.set(new byte[] { 1 }, new byte[] { 1, 2 });
		unlimitCache.set(new byte[] { 1, 2 }, new byte[] { 1, 2 });
		unlimitCache.set(new byte[] { 1 }, new byte[] { 1, 2, 3 });
		assertEquals(3 + 18 + 5, unlimitCache.getUsed());

		unlimitCache.delete(new byte[] { 1 });
		assertEquals(2 + 2 + 9, unlimitCache.getUsed());

		unlimitCache.incr(new byte[] { 3 }, 1, 0);
		assertEquals(13 + 1 + 4 + 9, unlimitCache.getUsed());
	}

	@Test
	public void testOperation() {
		byte[] key1 = new byte[] { 1 };
		byte[] key2 = new byte[] { 2 };
		byte[] key3 = new byte[] { 3 };

		unlimitCache.set(key1, new byte[] { 1, 2 });
		assertArrayEquals(new byte[] { 1, 2 }, unlimitCache.get(key1).getValue().getValue());
		assertNull(unlimitCache.get(key2).getValue());

		unlimitCache.delete(key1);
		assertNull(unlimitCache.get(key1).getValue());

		unlimitCache.incr(key3, 5, 0);
		unlimitCache.incr(key3, 10, 0);

		assertEquals(15, KeyValueUtil.bytesToInt(unlimitCache.get(key3).getValue().getValue()));
	}

	@Test
	public void testReplacement() {
		for (int i = 0; i < 128; i++) {
			limitCache.set(new byte[] { (byte) i }, new byte[] { (byte) i });
			assertTrue(limitCache.getUsed() <= 32);
		}
	}
	

}
