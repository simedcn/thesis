package com.ebay.kvstore.protocol;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProtocolCodeTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetMessage() {
		System.out.println(ProtocolCode.getMessage(0));
		System.out.println(ProtocolCode.getMessage(1));
		System.out.println(ProtocolCode.getMessage(2));
		System.out.println(ProtocolCode.getMessage(3));
		System.out.println(ProtocolCode.getMessage(4));
		System.out.println(ProtocolCode.getMessage(5));
		System.out.println(ProtocolCode.getMessage(6));

	}

}
