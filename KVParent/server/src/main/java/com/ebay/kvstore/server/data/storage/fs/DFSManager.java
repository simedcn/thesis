package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Used for manage the {@link DFSClient}
 * 
 * @author luochen
 * 
 */
public class DFSManager {

	private static FileSystem dfs = null;

	public static synchronized FileSystem getDFS() {
		if (dfs == null) {
			throw new NullPointerException("The DFS client has not been initialized properly");
		}
		return dfs;
	}

	@SuppressWarnings("deprecation")
	public static synchronized void init(InetSocketAddress addr, Configuration conf)
			throws IOException {
		if (dfs == null) {
			dfs = new DistributedFileSystem(addr, conf);
		}
	}

}
