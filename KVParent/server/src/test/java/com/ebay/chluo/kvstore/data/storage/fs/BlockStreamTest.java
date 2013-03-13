package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.data.storage.fs.DFSClientManager;
import com.ebay.chluo.kvstore.data.storage.fs.KVOutputStream;
import com.ebay.chluo.kvstore.data.storage.fs.KVFSInputIterator;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;
import static org.junit.Assert.*;

public class BlockStreamTest extends BaseFileTest {


	@Before
	public void init() {
	}

	@After
	public void dispose() {
		try {
			DFSClientManager.getClient().delete(path,false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testOperation() {
		int end = 0;
		DFSClient client = DFSClientManager.getClient();
		try {
			out = new KVOutputStream(client.create(path, true), blockSize);
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
			it = new KVFSInputIterator(0, end, blockSize, 0, client.open(path));
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
			it = new KVFSInputIterator(1, end, blockSize, 6, client.open(path));
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
