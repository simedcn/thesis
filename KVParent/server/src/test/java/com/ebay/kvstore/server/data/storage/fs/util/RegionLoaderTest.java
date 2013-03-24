package com.ebay.kvstore.server.data.storage.fs.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.BaseFileStorageTest;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.helper.IRegionLoadListener;
import com.ebay.kvstore.server.data.storage.helper.TaskManager;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public class RegionLoaderTest extends BaseFileStorageTest {
	protected Address addr;

	@Before
	public void setUp() throws Exception {
		conf.set(IConfigurationKey.Region_Block_Size, 64);
		addr = Address.parse(conf.get(IConfigurationKey.DataServer_Addr));
		region = new Region(0, new byte[] { 1 }, new byte[] { (byte) 0xff });
		storage = new RegionFileStorage(conf, region, true);

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRun() {
		for (int i = 0; i < 100; i++) {
			storage.storeInBuffer(new byte[] { (byte) i }, new byte[] { (byte) i });
		}
		storage.flush();
		for (int i = 0; i < 110; i += 15) {
			storage.storeInBuffer(new byte[] { (byte) i }, new byte[] { (byte) i });
		}
		storage.closeLogger();
		TaskManager.load(conf, new RegionLoadListener(), region);

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
