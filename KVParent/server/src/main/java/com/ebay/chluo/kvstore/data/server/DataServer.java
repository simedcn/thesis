package com.ebay.chluo.kvstore.data.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

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

import com.ebay.chluo.kvstore.IServer;
import com.ebay.chluo.kvstore.conf.MasterConfiguration;
import com.ebay.chluo.kvstore.conf.ServerConstants;
import com.ebay.chluo.kvstore.zookeeper.DataWatcher;

public class DataServer implements IServer {
	private static Logger logger = LoggerFactory.getLogger(DataServer.class);
	private int port;
	private String ip;
	private IoAcceptor acceptor;

	private int zkPort;
	private String zkIp;
	private ZooKeeper zooKeeper;
	private int sessionTimeout;
	private int connectTimeout;
	private DataClient client;

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
	public DataServer(MasterConfiguration conf) throws IOException {
		port = 2222;
		ip = "127.0.0.1";

		zkPort = 2181;
		zkIp = "127.0.0.1";
		sessionTimeout = 10 * 1000;

		connectTimeout = 2000;
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

		acceptor.bind(new InetSocketAddress(ip, port));
	}

	private void initZookeeper() throws Exception {
		zooKeeper = new ZooKeeper(zkIp + ':' + zkPort, sessionTimeout, new DataWatcher());
		if (zooKeeper.exists(ServerConstants.ZooKeeper_Data_Dir, false) == null) {
			// we suppose the /kvstore has been created by the master before
			zooKeeper.create(ServerConstants.ZooKeeper_Data_Dir, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		zooKeeper.create(ServerConstants.ZooKeeper_Data_Dir, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	private void initConnection() throws Exception {
		String masterAddr = new String(zooKeeper.getData(ServerConstants.ZooKeeper_Master_Dir,
				false, null));
		client = new DataClient(masterAddr, connectTimeout);
		client.connect();
	}

	public void run() throws Exception {
		initServer();
		initZookeeper();
		initConnection();

		IoSession session = client.connect();
		//session.write(new SimpleRequest("hello", "master"));
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
