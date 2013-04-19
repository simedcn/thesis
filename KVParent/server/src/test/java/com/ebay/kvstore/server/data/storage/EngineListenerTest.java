package com.ebay.kvstore.server.data.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.task.RegionTaskManager;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class EngineListenerTest extends BaseFileTest {

	private IStoreEngine engine;

	private Region region;

	@Before
	public void before() {
		region = new Region(0, new byte[] { 0 }, new byte[] { 1, 1, 1, 1, 1, 1, 1 });
		try {
			conf.set(IConfigurationKey.Storage_Policy, "persistent");
			engine = StoreEngineFactory.createStoreEngine(conf);
			engine.registerListener(new StoreLogListener());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@After
	public void after() {

	}

	@Test
	public void testListener() {
		try {
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
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
			engine.splitRegion(0, 5, null);
			while (RegionTaskManager.isRunning()) {
				Thread.sleep(100);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
