package com.ebay.kvstore.client;


public class BaseClientTest {
	protected IKVClient client;

	protected void initClient(ClientOption option) {
		client = KVClientFactory.getClient(option);
	}

}
