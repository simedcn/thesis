package com.ebay.kvstore.server.data.storage.fs.util;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.BaseFileStorageTest;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.helper.IRegionSplitListener;
import com.ebay.kvstore.server.data.storage.helper.RegionSplitter;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;

public class RegionSplitterTest extends BaseFileStorageTest {

	@Before
	public void setUp() throws IOException {
		conf.set(IConfigurationKey.Dataserver_Region_Block_Size, 64);
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
		RegionSplitter flusher = new RegionSplitter(storage, conf, new FlusherListener());
		flusher.run();
	}

	private class FlusherListener implements IRegionSplitListener {

		@Override
		public void onSplitBegin() {

		}

		@Override
		public Region onSplitEnd(boolean success, byte[] start, byte[] end) {
			Assert.assertTrue(success);
			return new Region(1, start, end);
		}

		@Override
		public void onSplitCommit(boolean success, IRegionStorage oldStorage,
				IRegionStorage newStorage) {
			Assert.assertTrue(success);
			try {
				KeyValue[] kvs = newStorage.getFromDisk(new byte[] { 60 });
				for (KeyValue kv : kvs) {
					System.out.println(kv);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
