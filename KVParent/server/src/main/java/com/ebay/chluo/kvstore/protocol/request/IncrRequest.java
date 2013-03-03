package com.ebay.chluo.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class IncrRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected int incremental;

	protected int initValue;

	@Override
	public int getType() {
		return ProtocolType.Incr;
	}

	public IncrRequest(byte[] key, int incremental, int initValue) {
		super();
		this.key = key;
		this.incremental = incremental;
		this.initValue = initValue;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public int getIncremental() {
		return incremental;
	}

	public void setIncremental(int incremental) {
		this.incremental = incremental;
	}

	public int getInitValue() {
		return initValue;
	}

	public void setInitValue(int initValue) {
		this.initValue = initValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + incremental;
		result = prime * result + initValue;
		result = prime * result + Arrays.hashCode(key);
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
		IncrRequest other = (IncrRequest) obj;
		if (incremental != other.incremental)
			return false;
		if (initValue != other.initValue)
			return false;
		if (!Arrays.equals(key, other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "IncrRequest [key=" + Arrays.toString(key) + ", incremental=" + incremental
				+ ", initValue=" + initValue + "]";
	}

}
