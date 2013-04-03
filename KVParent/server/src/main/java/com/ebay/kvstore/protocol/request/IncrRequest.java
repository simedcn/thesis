package com.ebay.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class IncrRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected int incremental;

	protected int initValue;

	public IncrRequest(byte[] key, int incremental, int initValue) {
		this.key = key;
		this.incremental = incremental;
		this.initValue = initValue;
	}

	public IncrRequest(byte[] key, int incremental, int initValue, boolean retry) {
		super(retry);
		this.key = key;
		this.incremental = incremental;
		this.initValue = initValue;
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

	public int getIncremental() {
		return incremental;
	}

	public int getInitValue() {
		return initValue;
	}

	public byte[] getKey() {
		return key;
	}

	@Override
	public int getType() {
		return IProtocolType.Incr_Req;
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

	public void setIncremental(int incremental) {
		this.incremental = incremental;
	}

	public void setInitValue(int initValue) {
		this.initValue = initValue;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "IncrRequest [key=" + Arrays.toString(key) + ", incremental=" + incremental
				+ ", initValue=" + initValue + "]";
	}

}
