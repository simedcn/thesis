package com.ebay.kvstore.server.data.storage.fs.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.storage.BaseFileStorageTest;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.KVFileInputIterator;
import com.ebay.kvstore.server.data.storage.fs.KVOutputStream;
import com.ebay.kvstore.server.data.storage.fs.RegionFileStorage;
import com.ebay.kvstore.server.data.storage.helper.IRegionFlushListener;
import com.ebay.kvstore.server.data.storage.helper.RegionFlusher;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

public class RegionFlusherTest extends BaseFileStorageTest {

	protected String path = "/kvstore/test/flush.data";

	protected List<String> logFiles = new ArrayList<>();

	@Before
	public void init() throws IOException {
		fout = DFSManager.getDFS().create(new Path(path), true);
		region = new Region(0, new byte[]{0},null);
	}

	@After
	public void dispose() {
	}

	@Test
	public void testRun() {
		final FileSystem fs = DFSManager.getDFS();
		try {

			out = new KVOutputStream(fout, blockSize);
			for (int i = 0; i < 100; i += 2) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			out.close();
			fin = fs.open(new Path(path));
			KeyValueCache cache = KeyValueCache.forBuffer();
			for (int i = 0; i < 100; i += 4) {
				cache.set(new byte[] { (byte) i }, new byte[] { (byte) (i + 1) });
			}
			cache.set(new byte[] { 0 }, new Value(null, true));
			// TODO
			RegionFlusher flusher = new RegionFlusher(new RegionFileStorage(conf, region), conf,
					new IRegionFlushListener() {
						@Override
						public void onFlushEnd(boolean success, String file) {
							try {
								assertTrue(success);
								it = new KVFileInputIterator(0, -1, blockSize, 0, fs.open(new Path(
										file)));
								int i = 0;
								while (it.hasNext()) {
									KeyValue kv = it.next();
									if (i != 0) {
										if (i % 4 == 0) {
											assertArrayEquals(new byte[] { (byte) (i + 1) }, kv
													.getValue().getValue());
										} else {
											assertArrayEquals(new byte[] { (byte) (i) }, kv
													.getValue().getValue());
										}
									}
									i += 2;
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void onFlushBegin() {

						}

						@Override
						public void onFlushCommit(boolean success, String path) {
							logFiles.add(path);
						}
					});
			flusher.run();

			flusher = new RegionFlusher(new RegionFileStorage(conf, region), conf, new IRegionFlushListener() {

				@Override
				public void onFlushEnd(boolean success, String file) {
					assertTrue(success);
					try {
						it = new KVFileInputIterator(0, -1, blockSize, 0, fs.open(new Path(file)));
						int i = 0;
						while (it.hasNext()) {
							KeyValue kv = it.next();
							assertArrayEquals(new byte[] { (byte) (i + 1) }, kv.getValue()
									.getValue());
							i += 4;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				@Override
				public void onFlushBegin() {

				}

				@Override
				public void onFlushCommit(boolean success, String file) {
					logFiles.add(file);
				}
			});
			flusher.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
