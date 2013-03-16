package com.ebay.kvstore.server.data.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.kvstore.Address;
import com.ebay.kvstore.kvstore.KeyValueUtil;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;

public class MemoryStoreEngineTest extends BaseFileTest {

	private IStoreEngine engine;

	private IStoreEngine engine2;

	private IConfiguration conf2;

	private Region region;

	@Before
	public void init() {
		try {
			conf2 = ConfigurationLoader.load();
			conf.set(IConfigurationKey.DataServer_Cache_Max, 128);
			conf2.set(IConfigurationKey.DataServer_Addr, new Address("192.1.1.1", 30000));
			conf2.set(IConfigurationKey.DataServer_Cache_Max, 4096);

			region = new Region(0, new byte[] { 0 }, new byte[] { 1, 1, 1, 1, 1, 1, 1 },
					new RegionStat());
			engine = StoreEngineFactory.getInstance().getMemoryStore(conf, region);
			engine.getRegions().add(region);

			engine2 = StoreEngineFactory.getInstance().getMemoryStore(conf2);

		} catch (IOException e) {
			e.printStackTrace();
		}

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
				assertTrue(engine.getMemoryUsed() < 128);
			}
		} catch (Exception e) {
		}
	}

	@Test
	public void testLoad() {
		try {
			engine.setCacheLimit(1000000);
			for (int i = 0; i < 100; i++) {
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i });
			}
			engine.unloadRegion(0);
			engine2.loadRegion(Address.parse(conf.get(IConfigurationKey.DataServer_Addr)), region);
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
				engine.set(new byte[] { (byte) i }, new byte[] { (byte) i });
			}
			engine.splitRegion(0, 1);
			assertEquals(2, engine.getRegions().size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
