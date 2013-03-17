package com.ebay.kvstore;

import java.net.InetSocketAddress;

public class Address {
	public String ip;
	public int port;

	@Override
	public String toString() {
		return ip + ":" + port;
	}

	public InetSocketAddress toInetSocketAddress() {
		return new InetSocketAddress(ip, port);
	}

	public Address(String ip, int port) {
		super();
		this.ip = ip;
		this.port = port;
	}

	public static Address parse(String addr) {
		try {
			String[] strs = addr.split(":");
			String ip = strs[0];
			int port = Integer.valueOf(strs[1]);
			return new Address(ip, port);
		} catch (Exception e) {
			throw new IllegalArgumentException("Bad format for " + addr
					+ ", the parameter should be ip:port");
		}
	}

}
