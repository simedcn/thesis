package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.ProtocolType;

public class GetResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected byte[] key;
	protected byte[] value;

	@Override
	public int getType() {
		return ProtocolType.Get_Resp;
	}

	public GetResponse(int retCode, byte[] key, byte[] value) {
		super(retCode);
		this.key = key;
		this.value = value;
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

	@Override
	public String toString() {
		return "GetResponse [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
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
		GetResponse other = (GetResponse) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

}
