package com.ebay.kvstore.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class KeyValueUtilTest{

	private byte[] a1 = new byte[] { 1, 2, 3, 4 };
	private byte[] a2 = new byte[] { 1 };
	private byte[] a3 = new byte[] { 2 };
	private byte[] a4 = new byte[] { 2, 3, 4 };

	private byte[] a5 = new byte[] { -10 };

	@Test
	public void testLess() {
		assertEquals(false, KeyValueUtil.less(a1, a2));
	}
	
	@Test
	public void testEqualsByteArrayByteArray() {
		assertEquals(false, KeyValueUtil.equals(a1, a2));
	}

	@Test
	public void testGreater() {
		assertEquals(true, KeyValueUtil.greater(a1, a2));
	}

	@Test
	public void testCompare() {
		assertEquals(1, KeyValueUtil.compare(a1, a2));
		assertEquals(0, KeyValueUtil.compare(a1, a1));
		assertEquals(1, KeyValueUtil.compare(a1, a4));
		assertEquals(-1, KeyValueUtil.compare(a2, a3));
		
		assertEquals(-1, KeyValueUtil.compare(a2, a5));

	}

}
