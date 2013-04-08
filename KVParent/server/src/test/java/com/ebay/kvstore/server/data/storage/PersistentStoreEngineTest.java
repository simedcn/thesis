package com.ebay.kvstore.server.data.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.helper.TaskManager;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;

public class PersistentStoreEngineTest extends BaseFileTest {

	protected IStoreEngine engine;
	protected Region region;
	protected static int id = 0;

	@Before
	public void setUp() throws Exception {
		conf.set(IConfigurationKey.Dataserver_Cache_Max, 128);
		conf.set(IConfigurationKey.Storage_Policy, "persistent");
	}

	@After
	public void tearDown() throws Exception {
		engine.dispose();
	}

	@Test
	public void testOperation() {
		try {
			region = new Region(0, new byte[] { 0 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
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
			region = new Region(1, new byte[] { 0 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i });
			}
			Region r = engine.unloadRegion(1);
			engine.loadRegion(r);
			for (byte i = 0; i < 100; i++) {
				try {
					assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i }).getValue()
							.getValue());
				} catch (Exception e) {
					e.printStackTrace();
					assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i }).getValue()
							.getValue());
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSplit() {
		try {
			region = new Region(2, new byte[] { 0 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i });
			}
			while (TaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.splitRegion(2, 3, null);
			while (TaskManager.isRunning()) {
				Thread.sleep(100);
			}
			for (byte i = 0; i < 100; i++) {
				assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i }).getValue()
						.getValue());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testStat() {
		try {
			region = new Region(4, new byte[] { 0 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
			engine.registerListener(new StoreStatListener());
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i });
			}
			engine.get(new byte[] { 0 });
			engine.incr(new byte[] { 100 }, 10, 0);
			while (TaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.stat();
			RegionStat stat = engine.getRegions().get(0).getStat();
			assertEquals(101, stat.writeCount);
			assertEquals(1, stat.readCount);
			assertFalse(stat.dirty);
			System.out.println(stat);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i });
			}
			engine.delete(new byte[] { 0 });
			engine.stat();
			System.out.println(stat);
			engine.unloadRegion(4);
			engine.loadRegion(region);
			engine.stat();
			System.out.println(stat);
			while (TaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.splitRegion(4, 5, null);
			while (TaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.stat();
			System.out.println(stat);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
