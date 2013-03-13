package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;

/**
 * Used for manage the {@link DFSClient}
 * 
 * @author luochen
 * 
 */
public class DFSClientManager {

	private static DFSClient client = null;

	public static synchronized void init(InetSocketAddress addr, Configuration conf)
			throws IOException {
		if (client != null) {
			throw new RuntimeException("The DFSClient has been inited!");
		}
		client = new DFSClient(addr, conf);
	}

	public static synchronized DFSClient getClient() {
		if (client == null) {
			throw new NullPointerException("The DFS client has not been initialized properly");
		}
		return client;
	}

}
