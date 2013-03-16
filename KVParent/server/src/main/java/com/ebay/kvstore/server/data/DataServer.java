package com.ebay.kvstore.server.data;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.ServerConstants;
import com.ebay.kvstore.kvstore.Address;
import com.ebay.kvstore.kvstore.IServer;
import com.ebay.kvstore.protocol.ProtocolType;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.zookeeper.DataWatcher;

public class DataServer implements IServer, IConfigurationKey {
	private static Logger logger = LoggerFactory.getLogger(DataServer.class);
	private Address dsAddr;
	private Address hdfsAddr;
	private Address zkAddr;
	private IoAcceptor acceptor;
	private ZooKeeper zooKeeper;
	private int zkSessionTimeout;
	private int connectTimeout;
	private DataClient client;

	private ProtocolDispatcher dispatcher;

	public static void main(String[] args) {
		DataServer server = null;
		try {
			server = new DataServer(null);
			server.run();
		} catch (Exception e) {
			logger.error("Fail to start master server", e);
			server.shutdown();
		}
	}

	// TODO
	public DataServer(IConfiguration conf) throws IOException {
		dsAddr = Address.parse(conf.get(DataServer_Addr));
		zkAddr = Address.parse(conf.get(ZooKeeper_Addr));
		hdfsAddr = Address.parse(conf.get(HDFS_Addr));
		zkSessionTimeout = conf.getInt(ZooKeeper_Session_Timeout);
		connectTimeout = conf.getInt(DataServer_Master_Timeout);

		dispatcher = new ProtocolDispatcher();
		dispatcher.registerHandler(ProtocolType.Set_Req, null);
		dispatcher.registerHandler(ProtocolType.Get_Req, null);
		dispatcher.registerHandler(ProtocolType.Delete_Req, null);
		dispatcher.registerHandler(ProtocolType.Incr_Req, null);
	}

	private void initServer() throws IOException {
		acceptor = new NioSocketAcceptor();
		// filter chain
		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
		// filter handler
		acceptor.setHandler(new MasterServerHandler());
		// config
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(dsAddr.toInetSocketAddress());
	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkAddr.toString(), zkSessionTimeout, new DataWatcher());
		if (zooKeeper.exists(ServerConstants.ZooKeeper_Data_Dir, false) == null) {
			// we suppose the /kvstore has been created by the master before
			zooKeeper.create(ServerConstants.ZooKeeper_Data_Dir, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		zooKeeper.create(ServerConstants.ZooKeeper_Data_Dir, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	private void initHDFS() throws IOException {
		DFSManager.init(hdfsAddr.toInetSocketAddress(), new Configuration());
	}

	private void initConnection() throws Exception {
		String masterAddr = new String(zooKeeper.getData(ServerConstants.ZooKeeper_Master_Dir,
				false, null));
		client = new DataClient(masterAddr, connectTimeout);
		client.connect();
	}

	public void run() throws Exception {
		initHDFS();
		initZookeeper();
		initConnection();
		initServer();

		// IoSession session = client.connect();
		// session.write(new SimpleRequest("hello", "master"));
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
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
			}
			client = null;
		}
	}

	private class MasterServerHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(MasterServerHandler.class);

		public void exceptionCaught(IoSession session, Throwable error) throws Exception {
			logger.error("Error occured with " + session.getRemoteAddress().toString(), error);

		}

		public void messageReceived(IoSession session, Object obj) throws Exception {
			System.out.println("Message received from " + session.getRemoteAddress().toString()
					+ " " + obj);
			if (obj.equals("quit")) {
				session.close(false);
				return;
			}
			session.write("Hello world!");
		}

		public void messageSent(IoSession session, Object arg1) throws Exception {

		}

		public void sessionClosed(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("Session closed " + session.getRemoteAddress().toString());
		}

		public void sessionCreated(IoSession session) throws Exception {
			// TODO Auto-generated method stub
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
