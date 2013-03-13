package com.ebay.chluo.kvstore.data.storage.fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.data.storage.cache.KeyValueCache;
import com.ebay.chluo.kvstore.data.storage.fs.DFSClientManager;
import com.ebay.chluo.kvstore.data.storage.fs.FlushListener;
import com.ebay.chluo.kvstore.data.storage.fs.KVOutputStream;
import com.ebay.chluo.kvstore.data.storage.fs.KeyValueFlusher;
import com.ebay.chluo.kvstore.data.storage.fs.KVFSInputIterator;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;
import com.ebay.chluo.kvstore.structure.Value;

public class KeyValueFileFlusherTest extends BaseFileTest {

	protected String path = "flush.data";

	protected List<String> logFiles = new ArrayList<>();

	@Before
	public void init() throws IOException {
		DFSClient client = DFSClientManager.getClient();
		fout = client.create(path, true);
	}

	@After
	public void dispose() {
	}

	@Test
	public void testRun() {
		DFSClient client = DFSClientManager.getClient();
		try {
			Region region = new Region(0, null, null, null);
			out = new KVOutputStream(fout, blockSize);
			for (int i = 0; i < 100; i += 2) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			out.close();
			fin = client.open(path);
			KeyValueCache cache = new KeyValueCache(0, null);
			for (int i = 0; i < 100; i += 4) {
				cache.set(new byte[] { (byte) i }, new byte[] { (byte) (i + 1) });
			}
			cache.set(new byte[] { 0 }, new Value(null, true));
			KeyValueFlusher flusher = new KeyValueFlusher(cache, "", path, region, blockSize,
					new FlushListener() {
						@Override
						public void onFlushEnd(boolean success, String file) {
							try {
								assertTrue(success);
								it = new KVFSInputIterator(0, -1, blockSize, 0, fin);
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

			flusher = new KeyValueFlusher(cache, "", null, region, blockSize, new FlushListener() {

				@Override
				public void onFlushEnd(boolean success, String file) {
					assertTrue(success);
					try {
						it = new KVFSInputIterator(0, -1, blockSize, 0, fin);
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
