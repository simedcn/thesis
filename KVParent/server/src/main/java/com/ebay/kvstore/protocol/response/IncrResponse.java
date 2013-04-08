package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class IncrResponse extends ClientResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected int incremental;

	protected int value;

	public IncrResponse(int retCode, byte[] key, int incremental, int value) {
		super(retCode);
		this.key = key;
		this.incremental = incremental;
		this.value = value;
	}

	public IncrResponse(int retCode, byte[] key, int incremental, int value, boolean retry) {
		super(retCode, retry);
		this.key = key;
		this.incremental = incremental;
		this.value = value;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IncrResponse other = (IncrResponse) obj;
		if (incremental != other.incremental)
			return false;
		if (!Arrays.equals(key, other.key))
			return false;
		if (value != other.value)
			return false;
		return true;
	}

	public int getIncremental() {
		return incremental;
	}

	public byte[] getKey() {
		return key;
	}

	@Override
	public int getType() {
		return IProtocolType.Incr_Resp;
	}

	public int getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + incremental;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + value;
		return result;
	}

	public void setIncremental(int incremental) {
		this.incremental = incremental;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "IncrResponse [key=" + Arrays.toString(key) + ", incremental=" + incremental
				+ ", value=" + value + "]";
	}

}
