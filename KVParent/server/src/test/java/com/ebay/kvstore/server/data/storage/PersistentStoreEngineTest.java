package com.ebay.kvstore.server.data.storage;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.kvstore.KeyValueUtil;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;

public class PersistentStoreEngineTest extends BaseFileTest {

	protected IStoreEngine engine;
	protected Region region;
	protected static int id = 0;

	@Before
	public void setUp() throws Exception {
		region = new Region(id++, new byte[] { 0 }, null, new RegionStat());
		engine = StoreEngineFactory.getInstance().getPersistentStore(conf, region);
	}

	@After
	public void tearDown() throws Exception {
		engine.dispose();
	}

	@Test
	public void testOperation() {

		try {
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i });
			}
			for (byte i = 0; i < 100; i += 2) {
				engine.delete(new byte[] { i });
			}
			for (byte i = 0; i < 100; i++) {
				if (i % 2 != 0) {
					assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i }).getValue()
							.getValue());
				} else {
					assertNull(engine.get(new byte[] { i }));
				}
			}

			engine.incr(new byte[] { 100 }, 5, 0);
			engine.incr(new byte[] { 100 }, 5, 0);
			engine.incr(new byte[] { 100 }, 5, 0);
			engine.incr(new byte[] { 100 }, 5, 0);
			assertEquals(20,
					KeyValueUtil.bytesToInt(engine.get(new byte[] { 100 }).getValue().getValue()));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testLoad() {
		try {
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i });
			}
			Region r = engine.unloadRegion(0);
			engine.loadRegion(addr, r);
			for (byte i = 0; i < 100; i++) {
				assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i }).getValue()
						.getValue());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
