package com.ebay.chluo.kvstore.data.storage.file;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;
import static org.junit.Assert.*;

public class BlockStreamTest extends BaseFileTest {

	@Before
	public void init() {
		file = new File(path);
	}

	@After
	public void dispose() {
		file.delete();
	}

	@Test
	public void testOperation() {
		int end = 0;
		try {
			out = new KVFileOutputStream(new FileOutputStream(file), blockSize);
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
			it = new KeyValueInputIterator(0, end, blockSize, 0, file);
			int counter = 0;
			while (it.hasNext()) {
				if (counter == 99) {
					System.out.println();
				}
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
			it = new KeyValueInputIterator(1, end, blockSize, 6, file);
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
