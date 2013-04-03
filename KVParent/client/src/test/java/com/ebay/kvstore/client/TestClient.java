package com.ebay.kvstore.client;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.ReadFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.junit.Test;

public class TestClient {

	@Test
	public static void main(String[] args) {
		try {
			IoConnector connector = new NioSocketConnector();
			connector.setConnectTimeoutMillis(2000);
			connector.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
			connector.getFilterChain().addLast("logger", new LoggingFilter());
			connector.setHandler(new Handler());
			connector.getSessionConfig().setUseReadOperation(true);
			ConnectFuture future = connector.connect(new InetSocketAddress(1000));
			future.awaitUninterruptibly();
			IoSession session = future.getSession();

			while (true) {
				session.write("I'm client");
				ReadFuture read = session.read();
				read.await();
				System.out.println(read.getMessage());
				Thread.sleep(2000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static class Handler implements IoHandler {

		@Override
		public void sessionCreated(IoSession session) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void sessionOpened(IoSession session) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void messageReceived(IoSession session, Object message) throws Exception {
		}

		@Override
		public void messageSent(IoSession session, Object message) throws Exception {
			// TODO Auto-generated method stub

		}

	}
}
