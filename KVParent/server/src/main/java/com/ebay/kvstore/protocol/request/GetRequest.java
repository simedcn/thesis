package com.ebay.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.kvstore.protocol.ProtocolType;

public class GetRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	@Override
	public int getType() {
		return ProtocolType.Get_Req;
	}

	public GetRequest(byte[] key) {
		super();
		this.key = key;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		GetRequest other = (GetRequest) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "GetRequest [key=" + Arrays.toString(key) + "]";
	}

}
