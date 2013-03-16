package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.ProtocolType;

public class IncrResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected int incremental;

	protected int value;

	@Override
	public int getType() {
		return ProtocolType.Incr_Resp;
	}

	public IncrResponse(int retCode, byte[] key, int incremental, int value) {
		super(retCode);
		this.key = key;
		this.incremental = incremental;
		this.value = value;
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

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
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

	@Override
	public String toString() {
		return "IncrResponse [key=" + Arrays.toString(key) + ", incremental=" + incremental
				+ ", value=" + value + "]";
	}

}
