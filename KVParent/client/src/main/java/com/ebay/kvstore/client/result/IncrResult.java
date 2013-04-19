package com.ebay.kvstore.client.result;

import com.ebay.kvstore.exception.KVException;

public class IncrResult extends BaseResult {

	private int value;

	public IncrResult(byte[] key, int value, KVException e) {
		super(key, e);
		this.value = value;
	}

	public IncrResult(byte[] key, int value) {
		super(key, null);
		this.value = value;
	}

	public int getValue() throws KVException {
		checkException();
		return value;
	}
}
