package com.ebay.kvstore.server.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FSUtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testValid() {
		assertTrue(FSUtil.isValidRegionFile("1-44294829.data"));
		assertTrue(FSUtil.isValidRegionFile("002-44294829.data"));
		assertTrue(FSUtil.isValidRegionLogFile("002-44294829.log"));
		assertTrue(FSUtil.isValidRegionLogFile("002-44294829.log"));

		assertFalse(FSUtil.isValidRegionLogFile("002-44294829.log1"));
		assertFalse(FSUtil.isValidRegionLogFile("002-44294829."));
		assertFalse(FSUtil.isValidRegionLogFile("002-.log"));

		assertFalse(FSUtil.isValidRegionFile("002-44294829.data1"));
		assertFalse(FSUtil.isValidRegionFile("002-44294829."));
		assertFalse(FSUtil.isValidRegionFile("002-.data"));
	}

	@Test
	public void testTimestamp() {
		assertEquals(1234567, FSUtil.getRegionFileTimestamp("1-1234567.data"));
		assertEquals(1234567, FSUtil.getRegionFileTimestamp("1-1234567.log"));
	}

}
