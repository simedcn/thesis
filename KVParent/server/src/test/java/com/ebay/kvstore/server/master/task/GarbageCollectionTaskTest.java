package com.ebay.kvstore.server.master.task;

import static org.junit.Assert.*;

import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.BaseTest;
import com.ebay.kvstore.PathBuilder;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.server.master.engine.MasterEngine;

public class GarbageCollectionTaskTest extends BaseTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testProcess() {
		try {
			FileSystem fs = DFSManager.getDFS();
			String checkpointDir = PathBuilder.getMasterCheckPointDir();
			String logDir = PathBuilder.getMasterLogDir();
			String dataDir = PathBuilder.getDataServerDir();
			Path checkpointPath = new Path(checkpointDir);
			Path logPath = new Path(logDir);
			Path dataPath = new Path(dataDir);
			Path regionPath = new Path(dataPath, "0");
			OutputStream out = fs.create(new Path(checkpointPath, "abc"), true);
			out.close();
			out = fs.create(new Path(checkpointPath, "10.ckp"), true);
			out.close();
			out = fs.create(new Path(logPath, "abc"), true);
			out.close();
			out = fs.create(new Path(logPath, "10.log"), true);
			out.close();
			out = fs.create(new Path(regionPath, "abc"), true);
			out.close();
			out = fs.create(new Path(regionPath, "10.log"), true);
			out.close();
			out = fs.create(new Path(regionPath, "10.data"), true);
			out.close();
			assertTrue(fs.exists(new Path(checkpointPath, "abc")));
			assertTrue(fs.exists(new Path(checkpointPath, "10.ckp")));
			assertTrue(fs.exists(new Path(logPath, "abc")));
			assertTrue(fs.exists(new Path(logPath, "10.log")));
			assertTrue(fs.exists(new Path(regionPath, "abc")));
			assertTrue(fs.exists(new Path(regionPath, "10.log")));
			assertTrue(fs.exists(new Path(regionPath, "10.data")));

			IMasterEngine engine = new MasterEngine(conf, null);
			GarbageCollectionTask task = new GarbageCollectionTask(conf, engine);
			task.process();
			assertFalse(fs.exists(new Path(checkpointPath, "abc")));
			assertFalse(fs.exists(new Path(checkpointPath, "10.ckp")));
			assertFalse(fs.exists(new Path(logPath, "abc")));
			assertFalse(fs.exists(new Path(logPath, "10.log")));
			assertFalse(fs.exists(new Path(regionPath, "abc")));
			assertFalse(fs.exists(new Path(regionPath, "10.log")));
			assertFalse(fs.exists(new Path(regionPath, "10.data")));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
