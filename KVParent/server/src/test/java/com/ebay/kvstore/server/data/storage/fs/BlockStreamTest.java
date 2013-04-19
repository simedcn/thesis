package com.ebay.kvstore.server.data.storage.fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.server.data.storage.BaseFileTest;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.KVFileIterator;
import com.ebay.kvstore.server.data.storage.fs.KVOutputStream;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

public class BlockStreamTest extends BaseFileTest {


	@Before
	public void init() {
	}

	@After
	public void dispose() {
		try {
			DFSManager.getDFS().delete(new Path(path),false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testOperation() {
		int end = 0;
		FileSystem fs = DFSManager.getDFS();
		try {
			out = new KVOutputStream(fs.create(new Path(path), true), blockSize);
			for (int i = 0; i < 100; i++) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			end = out.getCurrentBlock();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			it = new KVFileIterator(0, end, blockSize, 0, fs.open(new Path(path)));
			int counter = 0;
			while (it.hasNext()) {
				KeyValue kv = it.next();
				assertArrayEquals(new byte[] { (byte) counter }, kv.getKey());
				assertArrayEquals(new byte[] { (byte) counter }, kv.getValue().getValue());
				counter++;
			}
			assertEquals(100, counter);
		} catch (EOFException eof) {

		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			it = new KVFileIterator(2, end, blockSize, 10, fs.open(new Path(path)));
			int counter = 2;
			while (it.hasNext()) {
				KeyValue kv = it.next();
				assertArrayEquals(new byte[] { (byte) counter }, kv.getKey());
				assertArrayEquals(new byte[] { (byte) counter }, kv.getValue().getValue());
				counter++;
			}
			assertEquals(100, counter);
		} catch (EOFException eof) {

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
