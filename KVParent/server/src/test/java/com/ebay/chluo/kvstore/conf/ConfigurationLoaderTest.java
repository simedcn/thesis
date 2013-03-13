package com.ebay.chluo.kvstore.conf;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationLoaderTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testLoadString() {
		try {
			IConfiguration conf = ConfigurationLoader.load("kvstore.properties");
			assertEquals("lru", conf.get(IConfiguration.DataServer_Cache_Replacement_Policy));
			assertEquals(new Integer(180), conf.getInt(IConfiguration.GC_Check_Interval));
			assertEquals(new Integer(10), conf.getInt(IConfiguration.Checkpoint_Reserve_Days, 10));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testLoad() {
		try {
			IConfiguration conf = ConfigurationLoader.load();
			assertEquals("lru", conf.get(IConfiguration.DataServer_Cache_Replacement_Policy));
			assertEquals("127.0.0.1:20000", conf.get(IConfiguration.Master_Addr));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
