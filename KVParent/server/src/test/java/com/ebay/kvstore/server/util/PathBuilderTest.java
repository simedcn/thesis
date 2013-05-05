package com.ebay.kvstore.server.util;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PathBuilderTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetMasterCheckPointDir() {
		assertEquals("/kvstore/master/checkpoint/", PathBuilder.getMasterCheckPointDir());
	}

	@Test
	public void testGetMasterLogDir() {
		assertEquals("/kvstore/master/log/", PathBuilder.getMasterLogDir());
	}

	@Test
	public void testGetRegionDir() {
		assertEquals("/kvstore/data/1/", PathBuilder.getRegionDir(1));
	}

	@Test
	public void testGetRegionFilePath() {
		assertEquals("/kvstore/data/1/1-12345.data", PathBuilder.getRegionFilePath(1, 12345));

	}

	@Test
	public void testGetRegionLogPath() {
		assertEquals("/kvstore/data/1/1-12345.log", PathBuilder.getRegionLogPath(1, 12345));
	}

}
