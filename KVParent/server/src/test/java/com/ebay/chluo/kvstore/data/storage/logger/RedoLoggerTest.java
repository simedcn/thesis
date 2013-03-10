package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.File;
import java.io.FileInputStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedoLoggerTest {

	private final String path = "redo.log";

	private File file;

	@Before
	public void setUp() throws Exception {
		file = new File(path);
	}

	@After
	public void tearDown() throws Exception {
		file.delete();
	}

	@Test
	public void test() {
		try {
			IRedoLogger logger = new FileRedoLogger(file);
			for (byte i = 0; i < 50; i++) {
				logger.write(new SetMutation(new byte[] { i }, new byte[] { i }));
			}
			for (byte i = 0; i < 50; i++) {
				logger.write(new DeleteMutation(new byte[] { i }));
			}
			logger.close();
			LoggerInputIterator it = new LoggerInputIterator(new LoggerFileInputStream(
					new FileInputStream(file)));
			byte i = 0;
			while (it.hasNext()) {
				IMutation mutation = it.next();
				Assert.assertArrayEquals(new byte[] { (byte) (i % 50) }, mutation.getKey());
				i++;
			}
		} catch (Exception e) {

		}

	}
}
