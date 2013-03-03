package com.ebay.chluo.kvstore.protocol.request;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class DeleteRequest extends ClientRequest {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected byte[] key;

	@Override
	public int getType() {
		return ProtocolType.Delete;
	}

	public DeleteRequest(byte[] key) {
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
		DeleteRequest other = (DeleteRequest) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DeleteRequest [key=" + Arrays.toString(key) + "]";
	}

}
