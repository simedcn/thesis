package com.ebay.kvstore.conf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;

public class ConfigurationLoaderTest implements IConfigurationKey {

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
			assertEquals("lru", conf.get(DataServer_Cache_Replacement_Policy));
			assertEquals(new Integer(180), conf.getInt(GC_Check_Interval));
			assertEquals(new Integer(10), conf.getInt(Checkpoint_Reserve_Days, 10));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testLoad() {
		try {
			IConfiguration conf = ConfigurationLoader.load();
			assertEquals("lru", conf.get(DataServer_Cache_Replacement_Policy));
			assertEquals("127.0.0.1:20000", conf.get(Master_Addr));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
