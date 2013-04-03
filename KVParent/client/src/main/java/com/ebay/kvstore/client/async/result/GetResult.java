package com.ebay.kvstore.client.async.result;

import java.util.Arrays;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.exception.KVException;

public class GetResult extends BaseResult {

	private byte[] value;

	public GetResult(byte[] key, byte[] value, KVException e) {
		super(key, e);
		this.value = value;
	}

	public GetResult(byte[] key, byte[] value) {
		super(key, null);
		this.value = value;
	}

	public byte[] getValue() throws KVException {
		checkException();
		return value;
	}

	public int getCounter() throws KVException {
		checkException();
		if (value == null || value.length != 4) {
			throw new KVException("The key:" + Arrays.toString(key) + " is not a valid counter");
		}
		return KeyValueUtil.bytesToInt(value);
	}
}
