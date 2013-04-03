package com.ebay.kvstore.client.async.result;

import com.ebay.kvstore.exception.KVException;

public abstract class BaseResult {

	protected KVException e;
	protected byte[] key;

	public BaseResult(byte[] key, KVException e) {
		this.e = e;
		this.key = key;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKVException(KVException e) {
		this.e = e;
	}

	protected void checkException() throws KVException {
		if (e != null) {
			throw e;
		}
	}

}
