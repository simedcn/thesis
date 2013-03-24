package com.ebay.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class GetRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	public GetRequest(byte[] key) {
		super();
		this.key = key;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GetRequest other = (GetRequest) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		return true;
	}

	public byte[] getKey() {
		return key;
	}

	@Override
	public int getType() {
		return IProtocolType.Get_Req;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		return result;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "GetRequest [key=" + Arrays.toString(key) + "]";
	}

}
