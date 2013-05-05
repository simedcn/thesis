package com.ebay.kvstore.structure;

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

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Address other = (Address) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + port;
		return result;
	}

	public InetSocketAddress toInetSocketAddress() {
		return new InetSocketAddress(ip, port);
	}

	@Override
	public String toString() {
		return ip + ":" + port;
	}

}
