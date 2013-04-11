package com.ebay.kvstore.structure;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SystemInfoTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testUpdate() {
		try {
			SystemInfo info = new SystemInfo();
			System.out.println(info);
			Thread.sleep(1000);
			System.out.println(info);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
