package com.ebay.kvstore.client;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.junit.Test;

public class TestMina {

	@Test
	public static void main(String[] args) {
		try {
			IoBuffer newBuffer = IoBuffer.allocate(16);
			newBuffer.putDouble(-2.3);
			newBuffer.putLong(-4444L);
			newBuffer.flip();
			byte[] bytes = new byte[16];
			newBuffer.get(bytes);
			System.out.println(Arrays.toString(bytes));

			IoAcceptor acceptor = new NioSocketAcceptor();
			// filter chain
			acceptor.getFilterChain().addLast("logger", new LoggingFilter());
			acceptor.getFilterChain().addLast("executor", new ExecutorFilter());
			// filter handler
			acceptor.setHandler(new Handler());
			// config
			acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
			acceptor.getSessionConfig().setUseReadOperation(true);
			acceptor.bind(new InetSocketAddress(12345));
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
			System.out.println("Session opened" + session.getRemoteAddress());
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
			IoBuffer buffer = (IoBuffer) message;
			int value = buffer.getInt();
			System.out.println(value);
			IoBuffer newBuffer = IoBuffer.allocate(16);
			newBuffer.putDouble(-2.3);
			newBuffer.putLong(-4444L);
			newBuffer.flip();
			System.out.println(newBuffer.position());
			session.write(newBuffer);
		}

		@Override
		public void messageSent(IoSession session, Object message) throws Exception {

		}

	}
}
