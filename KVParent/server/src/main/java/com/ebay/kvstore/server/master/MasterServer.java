package com.ebay.kvstore.server.master;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.ServerConstants;
import com.ebay.kvstore.kvstore.Address;
import com.ebay.kvstore.kvstore.IServer;
import com.ebay.kvstore.protocol.ProtocolType;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.zookeeper.MasterWatcher;

public class MasterServer implements IServer, IConfigurationKey {
	private static Logger logger = LoggerFactory.getLogger(MasterServer.class);
	private Address masterAddr;
	private Address zkAddr;
	private Address hdfsAddr;
	private IoAcceptor acceptor;

	private ZooKeeper zooKeeper;

	private int zkSessionTimeout;
	private ProtocolDispatcher dispatcher;

	public static void main(String[] args) {
		try {
			IConfiguration conf = ConfigurationLoader.load();
			MasterServer server = new MasterServer(conf);
			server.run();
		} catch (Exception e) {
			logger.error("Fail to start master server", e);
		}
	}

	public MasterServer(IConfiguration conf) throws IOException {
		masterAddr = Address.parse(conf.get(Master_Addr));
		zkAddr = Address.parse(conf.get(ZooKeeper_Addr));
		hdfsAddr = Address.parse(conf.get(HDFS_Addr));
		zkSessionTimeout = conf.getInt(ZooKeeper_Session_Timeout);

		dispatcher = new ProtocolDispatcher();
		dispatcher.registerHandler(ProtocolType.Heart_Beart_Req, null);
		dispatcher.registerHandler(ProtocolType.Region_Table_Req, null);

		dispatcher.registerHandler(ProtocolType.Load_Region_Resp, null);
		dispatcher.registerHandler(ProtocolType.Unload_Region_Resp, null);
		dispatcher.registerHandler(ProtocolType.Split_Region_Resp, null);

	}

	private void initServer() throws IOException {
		acceptor = new NioSocketAcceptor();
		// filter chain
		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		// filter handler
		acceptor.setHandler(new MasterServerHandler());
		// config
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(masterAddr.toInetSocketAddress());
	}

	private void initHdfs() throws IOException {
		DFSManager.init(hdfsAddr.toInetSocketAddress(), new Configuration());
	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkAddr.toString(), zkSessionTimeout, new MasterWatcher());
		// create the path recursively...
		if (zooKeeper.exists(ServerConstants.ZooKeeper_Master_Dir, false) == null) {
			zooKeeper.create(ServerConstants.ZooKeeper_Base, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			zooKeeper.create(ServerConstants.ZooKeeper_Master_Dir, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		zooKeeper.create(ServerConstants.ZooKeeper_Master_Dir_Path, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		zooKeeper.setData(ServerConstants.ZooKeeper_Master_Dir, (masterAddr.toString()).getBytes(),
				-1);

	}

	public void shutdown() {
		if (acceptor != null) {
			acceptor.unbind();
			acceptor.dispose();
			acceptor = null;
		}
		if (zooKeeper != null) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e) {
			}
			zooKeeper = null;
		}
	}

	public void run() throws Exception {
		initZookeeper();
		initHdfs();
		initServer();

	}

	private class MasterServerHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(MasterServerHandler.class);

		public void exceptionCaught(IoSession session, Throwable error) throws Exception {
			logger.error("Error occured with " + session.getRemoteAddress().toString(), error);
		}

		public void messageReceived(IoSession session, Object obj) throws Exception {

		}

		public void messageSent(IoSession session, Object arg1) throws Exception {

		}

		public void sessionClosed(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session closed " + session.getRemoteAddress().toString());
		}

		public void sessionCreated(IoSession session) throws Exception {
			System.out.println("Session created " + session.getRemoteAddress().toString());

		}

		public void sessionIdle(IoSession session, IdleStatus arg1) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session idle " + session.getRemoteAddress().toString());

		}

		public void sessionOpened(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session opened " + session.getRemoteAddress().toString());

		}

	}

}
