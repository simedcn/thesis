package com.ebay.kvstore.server.data.storage.fs.task;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.BaseFileStorageTest;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.task.IRegionLoadListener;
import com.ebay.kvstore.server.data.storage.task.RegionTaskManager;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class RegionLoaderTest extends BaseFileStorageTest {
	protected Address addr;

	@Before
	public void setUp() throws Exception {
		conf.set(IConfigurationKey.Dataserver_Region_Block_Size, 64);
		addr = Address.parse(conf.get(IConfigurationKey.Dataserver_Addr));
		region = new Region(0, new byte[] { 1 }, new byte[] { (byte) 0xff });
		storage = new RegionFileStorage(conf, region, true);

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRun() {
		for (int i = 0; i < 100; i++) {
			storage.storeInBuffer(new byte[] { (byte) i }, new Value(new byte[] { (byte) i }));
		}
		for (int i = 0; i < 110; i += 15) {
			storage.storeInBuffer(new byte[] { (byte) i }, new Value(new byte[] { (byte) i }));
		}
		storage.closeLogger();
		RegionTaskManager.load(conf, new RegionLoadListener(), region);

	}

	private class RegionLoadListener implements IRegionLoadListener {

		@Override
		public void onLoadBegin() {

		}

		@Override
		public void onLoadEnd(boolean success) {
			Assert.assertTrue(success);

		}

		@Override
		public void onLoadCommit(boolean success, IRegionStorage storage) {
			assertTrue(success);
			KeyValue kv = storage.getFromBuffer(new byte[] { 15 });
			assertArrayEquals(new byte[] { 15 }, kv.getKey());
		}

	}

}
