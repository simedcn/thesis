package com.ebay.kvstore.client;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.ebay.kvstore.structure.Address;

public class ClientOption {
	private boolean sync = true;
	private int connectTimeout = 2000;// ms
	private int sessionTimeout = 30;// s

	private Set<Address> masterAddrs;

	public ClientOption(boolean sync, int connectTimeout, int sessionTimeout, Address... masterAddr) {
		super();
		this.sync = sync;
		this.connectTimeout = connectTimeout;
		this.sessionTimeout = sessionTimeout;
		this.masterAddrs = new HashSet<>();
		if (masterAddr != null) {
			for (Address addr : masterAddr) {
				masterAddrs.add(addr);
			}
		}
	}

	public int getConnectionTimeout() {
		return connectTimeout;
	}

	public Collection<Address> getMasterAddrs() {
		return masterAddrs;
	}

	public boolean isSync() {
		return sync;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

}
