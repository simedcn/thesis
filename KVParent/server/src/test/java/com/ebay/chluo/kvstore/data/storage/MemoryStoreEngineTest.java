package com.ebay.chluo.kvstore.data.storage;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.data.storage.IStoreEngine;
import com.ebay.chluo.kvstore.data.storage.StoreEngineFactory;
import com.ebay.chluo.kvstore.structure.Region;

public class MemoryStoreEngineTest {

	private IStoreEngine engine;

	@Before
	public void init() {
		engine = StoreEngineFactory.getInstance().getMemoryStore(128);
		engine.getRegions().add(
				new Region(0, new byte[] { 0 }, new byte[] { 1, 1, 1, 1, 1, 1, 1 }, null));
	}

	@Test
	public void testOperation() {
		byte[] key1 = new byte[] { 1 };
		byte[] key2 = new byte[] { 2 };
		try {
			engine.set(key1, key1);
			assertArrayEquals(key1, engine.get(key1).getValue().getValue());

			engine.delete(key1);
			assertNull(engine.get(key1).getValue());

			engine.incr(key2, 10, 0);
			engine.incr(key2, 2, 0);

			assertEquals(12, KeyValueUtil.bytesToInt(engine.get(key2).getValue().getValue()));
		} catch (Exception e) {
		}
	}

	@Test
	public void testMemoryUsage() {
		try {
			for (int i = 0; i < 100000; i++) {
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i });
				assertTrue(engine.getMemoryUsed()<128);
			}
		} catch (Exception e) {
		}
	}
}
