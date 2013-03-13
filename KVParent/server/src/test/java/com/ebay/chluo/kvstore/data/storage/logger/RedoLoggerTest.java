package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.chluo.kvstore.data.storage.fs.DFSClientManager;

public class RedoLoggerTest {

	private final String path = "/kvstore/test/redo.log";

	@BeforeClass
	public static void init() {
		try {
			DFSClientManager.init(new InetSocketAddress("localhost", 9000), new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			IRedoLogger logger = new FSRedoLogger(path);
			for (byte i = 0; i < 50; i++) {
				logger.write(new SetMutation(new byte[] { i }, new byte[] { i }));
			}
			for (byte i = 0; i < 50; i++) {
				logger.write(new DeleteMutation(new byte[] { i }));
			}
			logger.close();
			LoggerFSInputIterator it = new LoggerFSInputIterator(path);
			byte i = 0;
			while (it.hasNext()) {
				IMutation mutation = it.next();
				Assert.assertArrayEquals(new byte[] { (byte) (i % 50) }, mutation.getKey());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
