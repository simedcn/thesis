package com.ebay.kvstore;

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AddrUtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		InetSocketAddress addr = InetSocketAddress.createUnresolved("192.138.12.3", 8080);
		System.out.println(addr.getHostName());
		System.out.println(addr.getHostString());
		System.out.println(addr.getAddress().getHostAddress());
	}

}
