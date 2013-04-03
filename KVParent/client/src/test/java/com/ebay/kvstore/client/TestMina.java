package com.ebay.kvstore.client;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Date;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.junit.Test;

public class TestMina {

	@Test
	public static void main(String[] args) {
		try {
			IoAcceptor acceptor = new NioSocketAcceptor();
			// filter chain
			acceptor.getFilterChain().addLast("logger", new LoggingFilter());
			acceptor.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
			// filter handler
			acceptor.setHandler(new Handler());
			// config
			acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
			acceptor.getSessionConfig().setUseReadOperation(true);
			acceptor.bind(new InetSocketAddress(1000));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static class Handler implements IoHandler {
		@Override
		public void sessionCreated(IoSession session) throws Exception {

		}

		@Override
		public void sessionOpened(IoSession session) throws Exception {
		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {

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
			session.write(message + new Date().toLocaleString());
		}

		@Override
		public void messageSent(IoSession session, Object message) throws Exception {
			// TODO Auto-generated method stub

		}

	}
}
