package com.ebay.kvstore;

import com.ebay.kvstore.kvstore.KeyValueUtil;

import junit.framework.TestCase;

public class KeyValueUtilTest extends TestCase {

	private byte[] a1 = new byte[] { 1, 2, 3, 4 };
	private byte[] a2 = new byte[] { 1 };
	private byte[] a3 = new byte[] { 2 };
	private byte[] a4 = new byte[] { 2, 3, 4 };

	private byte[] a5 = new byte[] { -10 };

	public void testLess() {
		assertEquals(false, KeyValueUtil.less(a1, a2));
	}

	public void testEqualsByteArrayByteArray() {
		assertEquals(false, KeyValueUtil.equals(a1, a2));
	}

	public void testGreater() {
		assertEquals(true, KeyValueUtil.greater(a1, a2));
	}

	public void testCompare() {
		assertEquals(1, KeyValueUtil.compare(a1, a2));
		assertEquals(0, KeyValueUtil.compare(a1, a1));
		assertEquals(1, KeyValueUtil.compare(a1, a4));
		assertEquals(-1, KeyValueUtil.compare(a2, a3));
		
		assertEquals(-1, KeyValueUtil.compare(a2, a5));

	}

}
