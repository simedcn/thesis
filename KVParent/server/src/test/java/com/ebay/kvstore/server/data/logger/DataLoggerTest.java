package com.ebay.kvstore.server.data.logger;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.kvstore.server.logger.ILogger;
import com.ebay.kvstore.server.util.DFSManager;
import com.ebay.kvstore.structure.Value;

public class DataLoggerTest {

	private final String path = "/kvstore/test/redo.log";

	private final String newPath = "/kvstore/test/redo.new.log";

	@BeforeClass
	public static void init() {
		try {
			DFSManager.init(new InetSocketAddress("192.168.1.102", 9000), new Configuration());
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
			ILogger logger = DataFileLogger.forCreate(path);
			for (byte i = 0; i < 50; i++) {
				logger.write(new SetMutation(new byte[] { i },new Value( new byte[] { i })));
			}
			logger.renameTo(newPath);
			for (byte i = 0; i < 50; i++) {
				logger.write(new DeleteMutation(new byte[] { i }));
			}
			logger.close();
			logger = DataFileLogger.forAppend(newPath);
			for (byte i = 0; i < 50; i++) {
				logger.write(new SetMutation(new byte[] { i },new Value( new byte[] { i })));
			}
			logger.close();
			DataFileLoggerIterator it = new DataFileLoggerIterator(newPath);
			int i = 0;
			while (it.hasNext()) {
				IMutation mutation = it.next();
				Assert.assertArrayEquals(new byte[] { (byte) (i % 50) }, mutation.getKey());
				i++;
			}
			Assert.assertEquals(150, i);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
