package com.ebay.kvstore.client.result;

import com.ebay.kvstore.exception.KVException;

public class SetResult extends BaseResult {

	private byte[] value;

	public SetResult(byte[] key, byte[] value, KVException e) {
		super(key, e);
		this.value = value;
	}

	public byte[] getValue() throws KVException {
		checkException();
		return value;
	}

}
