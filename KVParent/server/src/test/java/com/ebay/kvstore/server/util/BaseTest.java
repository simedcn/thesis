package com.ebay.kvstore.server.util;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;

import com.ebay.kvstore.server.conf.ConfigurationLoader;
import com.ebay.kvstore.server.conf.IConfiguration;

public class BaseTest {
	protected static IConfiguration conf;
	static {
		try {
			conf = ConfigurationLoader.load();
			DFSManager.init(new InetSocketAddress("localhost", 9000), new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
