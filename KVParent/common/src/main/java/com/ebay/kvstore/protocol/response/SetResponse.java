package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class SetResponse extends ClientResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected byte[] value;

	protected int ttl;

	public SetResponse(int retCode, byte[] key, byte[] value) {
		super(retCode);
		this.key = key;
		this.value = value;
		this.ttl = 0;
	}

	public SetResponse(int retCode, byte[] key, byte[] value, int ttl, boolean retry) {
		super(retCode, retry);
		this.key = key;
		this.value = value;
		this.ttl = ttl;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SetResponse other = (SetResponse) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		if (ttl != other.ttl)
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

	public byte[] getKey() {
		return key;
	}

	public int getTtl() {
		return ttl;
	}

	@Override
	public int getType() {
		return IProtocolType.Set_Resp;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + ttl;
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "SetResponse [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ ", ttl=" + ttl + "]";
	}

}
