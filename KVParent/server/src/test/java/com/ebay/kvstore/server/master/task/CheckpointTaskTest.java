package com.ebay.kvstore.server.master.task;

import java.net.SocketAddress;

import org.apache.mina.core.session.DummySession;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.server.master.engine.MasterEngine;
import com.ebay.kvstore.server.util.BaseTest;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;
import static org.junit.Assert.*;

public class CheckpointTaskTest extends BaseTest {
	private Address zkAddr;
	private ZooKeeper zk = null;

	private class MockSession extends DummySession {
		@Override
		public SocketAddress getRemoteAddress() {
			return new Address("127.0.0.1", 100).toInetSocketAddress();
		}
	}

	@Before
	public void setUp() throws Exception {
		zkAddr = Address.parse(conf.get(IConfigurationKey.ZooKeeper_Addr));
		zk = new ZooKeeper(zkAddr.toString(), 2000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
			}
		});
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testProcess() {
		try {
			Address addr1 = new Address("127.0.0.1", 40);
			Address addr2 = new Address("127.0.0.1", 50);
			IMasterEngine engine = new MasterEngine(conf, zk);
			CheckpointTask task = new CheckpointTask(conf, engine);
			DataServerStruct struct = new DataServerStruct(addr1, 10);
			struct.addRegion(new Region(0, new byte[] { 0 }, new byte[] { 1 }));
			struct.addRegion(new Region(1, new byte[] { 10 }, new byte[] { 20 }));
			engine.addDataServer(struct, new MockSession());
			struct = new DataServerStruct(addr2, 10);
			struct.addRegion(new Region(3, new byte[] { 21 }, new byte[] { 30 }));
			struct.addRegion(new Region(4, new byte[] { 31 }, new byte[] { 40 }));
			engine.addDataServer(struct, new MockSession());
			task.process();
			engine = new MasterEngine(conf, zk);
			engine.start();
			assertEquals(4, engine.getUnassignedRegions().size());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
