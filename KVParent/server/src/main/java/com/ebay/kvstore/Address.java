package com.ebay.kvstore;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class Address implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static Address parse(SocketAddress remoteAddress) {
		String addr = remoteAddress.toString();
		if (addr.startsWith("/")) {
			addr = addr.substring(1);
		}
		return parse(addr);
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

	public String ip;

	public int port;

	public Address(String ip, int port) {
		super();
		this.ip = ip;
		this.port = port;
	}

	public InetSocketAddress toInetSocketAddress() {
		return new InetSocketAddress(ip, port);
	}

	@Override
	public String toString() {
		return ip + ":" + port;
	}

}
