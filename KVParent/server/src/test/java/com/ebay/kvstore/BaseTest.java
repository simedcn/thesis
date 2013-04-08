package com.ebay.kvstore;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;

import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;

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
