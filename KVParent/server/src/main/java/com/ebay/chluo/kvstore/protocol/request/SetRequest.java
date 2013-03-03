package com.ebay.chluo.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class SetRequest extends ClientRequest {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected byte[] value;

	protected int versionNumber;

	public SetRequest(byte[] key, byte[] value, int versionNumber) {
		super();
		this.key = key;
		this.value = value;
		this.versionNumber = versionNumber;
	}

	@Override
	public int getType() {
		return ProtocolType.Get;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public int getVersionNumber() {
		return versionNumber;
	}

	public void setVersionNumber(int versionNumber) {
		this.versionNumber = versionNumber;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + Arrays.hashCode(value);
		result = prime * result + versionNumber;
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
		if (!Arrays.equals(value, other.value))
			return false;
		if (versionNumber != other.versionNumber)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SetRequest [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ ", versionNumber=" + versionNumber + "]";
	}
}
