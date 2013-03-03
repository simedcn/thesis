package com.ebay.chluo.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class SetResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	protected byte[] value;

	protected int version;

	@Override
	public int getType() {
		return ProtocolType.Set;
	}

	public SetResponse(int retCode, byte[] key, byte[] value, int version) {
		super(retCode);
		this.key = key;
		this.value = value;
		this.version = version;
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

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + Arrays.hashCode(value);
		result = prime * result + version;
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
		SetResponse other = (SetResponse) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		if (version != other.version)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SetResponse [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ ", version=" + version + "]";
	}

}
