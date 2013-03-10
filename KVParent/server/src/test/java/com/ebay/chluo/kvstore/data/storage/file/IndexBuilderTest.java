package com.ebay.chluo.kvstore.data.storage.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;

public class IndexBuilderTest extends BaseFileTest {

	@Before
	public void setUp() throws Exception {
		file = new File("index.data");
	}

	@After
	public void tearDown() throws Exception {
		file.deleteOnExit();
	}

	@Test
	public void testBuild() {
		try {
			out = new KVFileOutputStream(new FileOutputStream(file), blockSize);
			for (int i = 0; i < 128; i++) {
				KeyValueUtil.writeToExternal(out, new KeyValue(new byte[] { (byte) i }, new Value(
						new byte[] { (byte) i })));
			}
			out.close();
			List<IndexEntry> index = IndexBuilder.build(file, blockSize, 5);
			for (IndexEntry e : index) {
				in = new KVFileInputStream(new FileInputStream(file), blockSize, e.blockStart, e.offset);
				Assert.assertArrayEquals(e.keyStart, KeyValueUtil.readFromExternal(in).getKey());
				in.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
