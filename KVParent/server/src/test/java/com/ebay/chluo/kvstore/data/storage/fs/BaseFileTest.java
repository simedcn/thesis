package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;

import com.ebay.chluo.kvstore.data.storage.fs.DFSClientManager;
import com.ebay.chluo.kvstore.data.storage.fs.BlockInputStream;
import com.ebay.chluo.kvstore.data.storage.fs.BlockOutputStream;
import com.ebay.chluo.kvstore.data.storage.fs.KVFSInputIterator;

public class BaseFileTest {
	protected File file;

	protected String path = "/kvstore/test/test.data";

	protected BlockOutputStream out;
	protected BlockInputStream in;
	
	protected KVFSInputIterator it;

	protected final int blockSize = 16;

	protected OutputStream fout;
	
	protected InputStream fin;
	
	@BeforeClass
	public static void initClass() {
		try {
			DFSClientManager.init(new InetSocketAddress("localhost", 9000), new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
