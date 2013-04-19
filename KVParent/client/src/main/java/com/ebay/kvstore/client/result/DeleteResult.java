package com.ebay.kvstore.client.result;

import com.ebay.kvstore.exception.KVException;

public class DeleteResult extends BaseResult {

	public DeleteResult(byte[] key, KVException e) {
		super(key, e);
	}

	public DeleteResult(byte[] key) {
		super(key, null);
	}

	public boolean isSuccess() throws KVException {
		checkException();
		return true;
	}

}
