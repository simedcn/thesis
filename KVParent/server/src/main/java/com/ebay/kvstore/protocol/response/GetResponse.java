package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class GetResponse extends ClientResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected byte[] key;
	protected byte[] value;

	public GetResponse(int retCode, byte[] key, byte[] value) {
		super(retCode);
		this.key = key;
		this.value = value;
	}

	public GetResponse(int retCode, byte[] key, byte[] value, boolean retry) {
		super(retCode);
		this.key = key;
		this.value = value;
		this.retry = retry;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GetResponse other = (GetResponse) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

	public byte[] getKey() {
		return key;
	}

	@Override
	public int getType() {
		return IProtocolType.Get_Resp;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "GetResponse [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ "]";
	}

}
