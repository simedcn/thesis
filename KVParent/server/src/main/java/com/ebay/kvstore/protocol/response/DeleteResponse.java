package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.IProtocolType;

public class DeleteResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected byte[] key;

	public DeleteResponse(int retCode, byte[] key) {
		super(retCode);
		this.key = key;
	}

	public DeleteResponse(int retCode, byte[] key, boolean retry) {
		super(retCode, retry);
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
		DeleteResponse other = (DeleteResponse) obj;
		if (!Arrays.equals(key, other.key))
			return false;
		return true;
	}

	public byte[] getKey() {
		return key;
	}

	@Override
	public int getType() {
		return IProtocolType.Delete_Resp;
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
		return "DeleteResponse [key=" + Arrays.toString(key) + "]";
	}

}
