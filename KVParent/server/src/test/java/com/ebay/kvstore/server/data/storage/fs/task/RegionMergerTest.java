package com.ebay.kvstore.server.data.storage.fs.task;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.BaseFileStorageTest;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.task.IRegionMergeListener;
import com.ebay.kvstore.server.data.storage.task.RegionMerger;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class RegionMergerTest extends BaseFileStorageTest {

	private Region region1;
	private Region region2;

	private IRegionStorage storage1;
	private IRegionStorage storage2;

	@Before
	public void setUp() throws IOException {
		conf.set(IConfigurationKey.Dataserver_Region_Block_Size, 64);
		region1 = new Region(0, new byte[] { 1 }, new byte[] { (byte) 49 });
		storage1 = new RegionFileStorage(conf, region1, true);

		region2 = new Region(1, new byte[] { 50 }, null);
		storage2 = new RegionFileStorage(conf, region2, true);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRun() {
		for (int i = 0; i < 50; i++) {
			storage1.storeInBuffer(new byte[] { (byte) i }, new Value(new byte[] { (byte) i }));
		}
		for (int i = 50; i < 100; i++) {
			storage2.storeInBuffer(new byte[] { (byte) i }, new Value(new byte[] { (byte) i }));
		}
		RegionMerger merger = new RegionMerger(conf, storage1, storage2,
				new IRegionMergeListener() {

					@Override
					public Region onMergeEnd(boolean success, Region region1, Region region2) {
						assertTrue(success);
						byte[] start = region1.getStart();
						byte[] end = region2.getEnd();
						return new Region(3, start, end);
					}

					@Override
					public void onMergeCommit(boolean success, IRegionStorage storage) {
						assertTrue(success);
						try {
							for (byte i = 0; i < 100; i++) {
								byte[] key = new byte[] { i };
								KeyValue[] kvs = storage.getFromDisk(key);
								boolean found = false;
								for (KeyValue kv : kvs) {
									if (Arrays.equals(kv.getKey(), key)) {
										assertArrayEquals(key, kv.getValue().getValue());
										found = true;
										break;
									}
								}
								assertTrue(found);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void onMergeBegin() {

					}
				});
		merger.run();
	}
}
