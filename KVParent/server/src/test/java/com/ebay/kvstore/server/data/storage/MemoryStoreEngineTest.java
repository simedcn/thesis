package com.ebay.kvstore.server.data.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.server.conf.ConfigurationLoader;
import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.util.KeyValueUtil;

public class MemoryStoreEngineTest extends BaseFileTest {

	private IStoreEngine engine;

	private IStoreEngine engine2;

	private IConfiguration conf2;

	private Region region;

	@Before
	public void init() {
		try {
			conf2 = ConfigurationLoader.load();
			conf.set(IConfigurationKey.Storage_Policy, "memory");
			conf2.set(IConfigurationKey.Storage_Policy, "memory");
			conf.set(IConfigurationKey.Dataserver_Cache_Max, 4);
			conf2.set(IConfigurationKey.Dataserver_Addr, new Address("192.1.1.1", 30000));
			conf2.set(IConfigurationKey.Dataserver_Cache_Max, 4);

			region = new Region(0, new byte[] { 0 }, new byte[] { 1, 1, 1, 1, 1, 1, 1 });
			engine = StoreEngineFactory.createStoreEngine(conf);
			engine.addRegion(region, true);
			engine2 = StoreEngineFactory.createStoreEngine(conf2);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testOperation() {
		byte[] key1 = new byte[] { 1 };
		byte[] key2 = new byte[] { 2 };
		try {
			engine.set(key1, key1, 0);
			assertArrayEquals(key1, engine.get(key1).getValue().getValue());

			engine.delete(key1);
			assertNull(engine.get(key1).getValue());

			engine.incr(key2, 10, 0, 0);
			engine.incr(key2, 2, 0, 0);

			assertEquals(12, KeyValueUtil.bytesToInt(engine.get(key2).getValue().getValue()));
		} catch (Exception e) {
		}
	}

	@Test
	public void testMemoryUsage() {
		try {
			for (int i = 0; i < 100000; i++) {
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i }, 0);
				System.out.println(engine.getMemoryUsed());
				assertTrue(engine.getMemoryUsed() < 4096);
			}
		} catch (Exception e) {
		}
	}

	@Test
	public void testLoad() {
		try {
			engine.setCacheLimit(1000000);
			for (int i = 0; i < 100; i++) {
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i },0);
			}
			engine.unloadRegion(0);
			engine2.loadRegion(region);
			assertArrayEquals(new byte[] { 0 }, engine2.get(new byte[] { 0 }).getValue().getValue());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSplit() {
		try {
			engine.setCacheLimit(1000000);
			for (int i = 0; i < 100; i++) {
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i },0);
			}
			engine.splitRegion(0, 1, null);
			assertEquals(2, engine.getAllRegions().length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMerge() {
		try {
			engine.unloadRegion(0);
			Region region = new Region(0, new byte[] { 0 }, new byte[] { 50 });
			Region region2 = new Region(1, new byte[] { 51 }, null);
			engine.addRegion(region, true);
			engine.addRegion(region2, true);
			for (byte i = 0; i < 100; i++) {
				engine.set(new byte[] { i }, new byte[] { i },0);
			}
			engine.mergeRegion(0, 1, 2, new IRegionMergeCallback() {
				@Override
				public void callback(boolean success, int region1, int region2, Region region) {
					assertTrue(success);
					engine.unloadRegion(2);

					try {
						engine.loadRegion(region);
						for (byte i = 0; i < 100; i++) {
							assertArrayEquals(new byte[] { i }, engine.get(new byte[] { i })
									.getValue().getValue());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
		} catch (Exception e) {

		}
	}

	@Test
	public void testStat() {
		try {
			engine.registerListener(new StoreStatListener());
			RegionStat stat = null;
			for (int i = 0; i < 10; i++) {
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i },0);
			}
			engine.get(new byte[] { 0 });
			engine.incr(new byte[] { 11 }, 10, 0,0);
			engine.stat();
			stat = engine.getAllRegions()[0].getStat();
			assertEquals(11, stat.writeCount);
			assertEquals(1, stat.readCount);
			assertEquals(11, stat.keyNum);
			assertEquals(201, stat.size);
			assertFalse(stat.dirty);
			engine.set(new byte[] { (byte) 0 }, new byte[] { (byte) 0 },0);
			assertTrue(stat.dirty);
			assertEquals(12, stat.writeCount);
			assertEquals(1, stat.readCount);
			assertEquals(11, stat.keyNum);
			assertEquals(201, stat.size);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
