package com.ebay.kvstore.server.data;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.protocol.ProtocolType;
import com.ebay.kvstore.protocol.handler.ProtocolDispatcher;

public class DataClient {
	private String ip;
	private int port;
	private int timeout;
	private IoSession session;

	private ProtocolDispatcher dispatcher;

	@SuppressWarnings("unused")
	public static void main(String[] args) throws InterruptedException {
		try {
			DataClient client = new DataClient("127.0.0.1", 1111, 2000);
			IoSession session = client.connect();
			Thread.sleep(1000);
			/*
			 * SimpleRequest request = new SimpleRequest("name", "luochen");
			 * session.write(request);
			 */
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public DataClient(String ip, int port, int timeout) {
		this.ip = ip;
		this.port = port;
		this.timeout = timeout;

		this.dispatcher = new ProtocolDispatcher();

		dispatcher.registerHandler(ProtocolType.Load_Region_Req, null);
		dispatcher.registerHandler(ProtocolType.Unload_Region_Req, null);
		dispatcher.registerHandler(ProtocolType.Split_Region_Req, null);
		dispatcher.registerHandler(ProtocolType.Heart_Beart_Resp, null);

	}

	public DataClient(String path, int timeout) {
		String[] s = path.split(":");
		this.ip = s[0];
		this.port = Integer.valueOf(s[1]);
		this.timeout = timeout;
	}

	public IoSession connect() throws IOException {
		if (session == null) {
			NioSocketConnector connector = new NioSocketConnector();
			connector.setConnectTimeoutMillis(timeout);
			connector.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

			connector.getFilterChain().addLast("logger", new LoggingFilter());
			connector.setHandler(new DataClientHandler());
			ConnectFuture future = connector.connect(new InetSocketAddress(ip, port));
			future.awaitUninterruptibly();
			session = future.getSession();
		}
		return session;
	}

	public void close() throws IOException {
		session.close(false);
		session = null;
	}

	class DataClientHandler implements IoHandler {
		private Logger logger = LoggerFactory.getLogger(DataClientHandler.class);

		public void exceptionCaught(IoSession session, Throwable error) throws Exception {
			logger.error("Error occured with " + session.getRemoteAddress().toString(), error);

		}

		public void messageReceived(IoSession session, Object obj) throws Exception {
			/*
			 * if (obj instanceof SimpleResponse) {
			 * System.out.println("Message received from " +
			 * session.getRemoteAddress().toString() + " " + obj); } else {
			 * System.err.println("Malformat message " + obj); }
			 */
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

		public void messageSent(IoSession session, Object message) throws Exception {
			// TODO Auto-generated method stub

		}
	}
}
