package com.ebay.kvstore.client;

import com.ebay.kvstore.client.async.AsyncKVClient;
import com.ebay.kvstore.client.sync.SyncKVClient;

public class KVClientFactory {
	public static IKVClient getClient(ClientOption option) {
		boolean sync = option.isSync();
		if (sync) {
			return new SyncKVClient(option);
		} else {
			return new AsyncKVClient(option);
		}
	}
}
