package com.ebay.kvstore.server.data.storage;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.conf.ConfigurationLoader;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.server.data.storage.fs.IBlockInputStream;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.server.data.storage.fs.KVFileIterator;

public class BaseFileTest {
	protected File file;

	protected String path = "/kvstore/test/test.data";

	protected IBlockOutputStream out;
	protected IBlockInputStream in;

	protected KVFileIterator it;

	protected final int blockSize = 16;

	protected OutputStream fout;

	protected InputStream fin;

	protected static IConfiguration conf;

	protected static Address addr;

	@BeforeClass
	public static void initClass() {
		try {
			conf = ConfigurationLoader.load();
			addr = Address.parse(conf.get(IConfigurationKey.DataServer_Addr));
			DFSManager.init(new InetSocketAddress("localhost", 9000), new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
