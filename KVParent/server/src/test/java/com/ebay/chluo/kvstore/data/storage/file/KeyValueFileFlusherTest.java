package com.ebay.chluo.kvstore.data.storage.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.data.storage.cache.KeyValueCache;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;
import com.ebay.chluo.kvstore.structure.Value;

public class KeyValueFileFlusherTest extends BaseFileTest {

	protected String path = "flush.data";

	protected List<File> logFiles = new ArrayList<>();

	@Before
	public void init() {
		file = new File(path);
	}

	@After
	public void dispose() {
		file.delete();
		for (File f : logFiles) {
			f.delete();
		}
	}

	@Test
	public void testRun() {
		try {
			Region region = new Region(0, null, null, null);
			out = new KVFileOutputStream(new FileOutputStream(file), blockSize);
			for (int i = 0; i < 100; i += 2) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			out.close();

			KeyValueCache cache = new KeyValueCache(0, null);
			for (int i = 0; i < 100; i += 4) {
				cache.set(new byte[] { (byte) i }, new byte[] { (byte) (i + 1) });
			}
			cache.set(new byte[] { 0 }, new Value(null, true));
			KeyValueFileFlusher flusher = new KeyValueFileFlusher(cache, new File(path), region,
					blockSize, new FlushListener() {
						@Override
						public void onFlushEnd(boolean success, File file) {
							try {
								assertTrue(success);
								it = new KeyValueInputIterator(0, -1, blockSize, 0, file);
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
						public void onFlushCommit(File file) {
							logFiles.add(file);							
						}
					});
			flusher.run();

			flusher = new KeyValueFileFlusher(cache, null, region, blockSize, new FlushListener() {

				@Override
				public void onFlushEnd(boolean success, File file) {
					assertTrue(success);
					try {
						it = new KeyValueInputIterator(0, -1, blockSize, 0, file);
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
				public void onFlushCommit(File file) {
					logFiles.add(file);					
				}
			});
			flusher.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
