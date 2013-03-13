package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.data.storage.fs.DFSClientManager;
import com.ebay.chluo.kvstore.data.storage.fs.IndexBuilder;
import com.ebay.chluo.kvstore.data.storage.fs.IndexEntry;
import com.ebay.chluo.kvstore.data.storage.fs.KVInputStream;
import com.ebay.chluo.kvstore.data.storage.fs.KVOutputStream;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;

public class IndexBuilderTest extends BaseFileTest {

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		DFSClientManager.getClient().delete(path, false);
	}

	@Test
	public void testBuild() {
		DFSClient client = DFSClientManager.getClient();
		try {
			out = new KVOutputStream(client.create(path, true), blockSize);
			for (int i = 0; i < 128; i++) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			out.close();
			List<IndexEntry> index = IndexBuilder.build(client.open(path), blockSize, 5);
			for (IndexEntry e : index) {
				in = new KVInputStream(client.open(path), blockSize, e.blockStart, e.offset);
				Assert.assertArrayEquals(e.keyStart, KeyValueUtil.readFromExternal(in).getKey());
				in.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
