package com.ebay.kvstore.server.master.logger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.kvstore.BaseTest;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.structure.Address;

public class OperationLoggerTest extends BaseTest{


	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testForCreate() throws IOException {
		String path = "/test/master.test.log";
		String newPath = "/test/master.test.new.log";
		Address addr = Address.parse("127.0.0.1:1000");
		DFSManager.getDFS().mkdirs(new Path("/test/"));
		IOperationLogger logger = OperationFileLogger.forCreate(path);
		for (byte i = 0; i < 10; i++) {
			logger.write(new LoadOperation(i, addr));
			logger.write(new UnloadOperation(i, addr));
			logger.write(new SplitOperation(i, i + 1, addr, new byte[] { i }));
		}
		logger.renameTo(newPath);
		for (byte i = 10; i < 20; i++) {
			logger.write(new LoadOperation(i, addr));
			logger.write(new UnloadOperation(i, addr));
			logger.write(new SplitOperation(i, i + 1, addr, new byte[] { i }));
		}
		logger.close();
		OperationFileLoggerIterator it = new OperationFileLoggerIterator(newPath);
		int i = 0;
		while (it.hasNext()) {
			IOperation op = it.next();
			switch (i % 3) {
			case 0:
				LoadOperation lop = (LoadOperation) op;
				assertEquals(i / 3, lop.getRegionId());
				break;
			case 1:
				UnloadOperation uop = (UnloadOperation) op;
				assertEquals(i / 3, uop.getRegionId());
				break;
			case 2:
				SplitOperation sop = (SplitOperation) op;
				assertEquals(i / 3, sop.getRegionId());
				assertArrayEquals(new byte[] { (byte) (i / 3) }, sop.getOldKeyEnd());
				break;
			default:
				break;
			}
			assertEquals(addr, op.getAddr());
			i++;
		}
		assertEquals(60, i);
		DFSManager.getDFS().delete(new Path(newPath), false);
	}

}
