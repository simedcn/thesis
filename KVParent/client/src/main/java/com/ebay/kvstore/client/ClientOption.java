package com.ebay.kvstore.client;

import com.ebay.kvstore.Address;

public class ClientOption {
	private boolean sync = true;
	private int connectionTimeout = 2000;// ms
	private Address masterAddr;
	
	public ClientOption(boolean sync, int connectionTimeout, Address masterAddr) {
		super();
		this.sync = sync;
		this.connectionTimeout = connectionTimeout;
		this.masterAddr = masterAddr;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public Address getMasterAddr() {
		return masterAddr;
	}

	public boolean isSync() {
		return sync;
	}

	
}
