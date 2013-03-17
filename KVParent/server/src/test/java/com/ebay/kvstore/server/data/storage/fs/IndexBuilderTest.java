package com.ebay.kvstore.server.data.storage.fs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.server.data.storage.BaseFileTest;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

public class IndexBuilderTest extends BaseFileTest {

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		DFSManager.getDFS().delete(new Path(path), false);
	}

	@Test
	public void testBuild() {
		FileSystem fs = DFSManager.getDFS();
		try {
			out = new KVOutputStream(fs.create(new Path(path), true), blockSize);
			for (int i = 0; i < 128; i++) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			out.close();
			List<IndexEntry> index = new ArrayList<>();
			IndexBuilder.build(index, path, blockSize, 5);
			for (IndexEntry e : index) {
				in = new KVInputStream(fs.open(new Path(path)), blockSize, e.blockStart, e.offset);
				Assert.assertArrayEquals(e.keyStart, KeyValueUtil.readFromExternal(in).getKey());
				in.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
