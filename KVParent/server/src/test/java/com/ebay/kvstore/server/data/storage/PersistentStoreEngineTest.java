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
import com.ebay.kvstore.server.data.storage.task.RegionTaskManager;
import com.ebay.kvstore.structure.KeyValue;
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
			engine.addRegion(region, true);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i }, 0);
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

			engine.incr(new byte[] { 100 }, 5, 0, 0);
			engine.incr(new byte[] { 100 }, 5, 0, 0);
			engine.incr(new byte[] { 100 }, 5, 0, 0);
			engine.incr(new byte[] { 100 }, 5, 0, 0);
			assertEquals(20,
					KeyValueUtil.bytesToInt(engine.get(new byte[] { 100 }).getValue().getValue()));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testTTL() {
		try {
			long time = System.currentTimeMillis();
			region = new Region(0, new byte[] { 0 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
			engine.addRegion(region, true);
			byte[] key1 = new byte[] { 1 };
			byte[] key2 = new byte[] { 2 };
			engine.set(key1, key2, 1000);
			Thread.sleep(500);
			KeyValue kv = engine.get(key1);
			int ttl = KeyValueUtil.getTtl(kv.getValue().getExpire());
			assertEquals(true, ttl <= 500);
			Thread.sleep(1000);
			kv = engine.get(key1);
			assertNull(kv);
			engine.incr(key2, 1, 0, 1000);
			Thread.sleep(500);
			kv = engine.incr(key2, 1, 0, 500);
			int counter = KeyValueUtil.bytesToInt(kv.getValue().getValue());
			assertEquals(2, counter);
			Thread.sleep(1000);
			kv = engine.incr(key2, 1, 0, 500);
			counter = KeyValueUtil.bytesToInt(kv.getValue().getValue());
			assertEquals(1, counter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testLoad() {
		try {
			region = new Region(1, new byte[] { 0 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
			engine.addRegion(region, true);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i }, 0);
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
			engine.addRegion(region, true);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i }, 0);
			}
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.splitRegion(2, 3, null);
			while (RegionTaskManager.isRunning()) {
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
			engine.addRegion(region, true);
			engine.registerListener(new StoreStatListener());
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i }, 0);
			}
			engine.get(new byte[] { 0 });
			engine.incr(new byte[] { 100 }, 10, 0, 0);
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.stat();
			RegionStat stat = engine.getAllRegions()[0].getStat();
			assertEquals(101, stat.writeCount);
			assertEquals(1, stat.readCount);
			assertFalse(stat.dirty);
			System.out.println(stat);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i }, 0);
			}
			engine.delete(new byte[] { 0 });
			engine.stat();
			System.out.println(stat);
			engine.unloadRegion(4);
			engine.loadRegion(region);
			engine.stat();
			System.out.println(stat);
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.splitRegion(4, 5, null);
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.stat();
			System.out.println(stat);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMerge() {
		try {
			Region region1 = new Region(5, new byte[] { 0 }, new byte[] { 49 });
			Region region2 = new Region(6, new byte[] { 50 }, null);
			engine = StoreEngineFactory.createStoreEngine(conf);
			engine.addRegion(region1, true);
			engine.addRegion(region2, true);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i }, 0);
			}
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.mergeRegion(5, 6, 7, new IRegionMergeCallback() {
				@Override
				public void callback(boolean success, int region1, int region2, Region region) {
					try {
						assertEquals(true, success);
						assertEquals(1, engine.getAllRegions().length);
						for (byte i = 0; i < 100; i++) {
							assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i })
									.getValue().getValue());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
