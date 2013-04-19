package com.ebay.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class SetRequest extends ClientRequest {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected byte[] value;

	protected int ttl;

	public SetRequest(byte[] key, byte[] value, int ttl) {
		this.key = key;
		this.value = value;
		this.ttl = ttl;
	}

	public SetRequest(byte[] key, byte[] value, int ttl, boolean retry) {
		super(retry);
		this.key = key;
		this.value = value;
		this.ttl = ttl;
	}

	public byte[] getKey() {
		return key;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	@Override
	public int getType() {
		return IProtocolType.Set_Req;
	}

	public byte[] getValue() {
		return value;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "SetRequest [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ ", ttl=" + ttl + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + (int) (ttl ^ (ttl >>> 32));
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SetRequest other = (SetRequest) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		if (ttl != other.ttl)
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

}