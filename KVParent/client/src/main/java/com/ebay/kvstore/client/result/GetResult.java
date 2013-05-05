package com.ebay.kvstore.client.result;

import java.util.Arrays;

import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.util.KeyValueUtil;

public class GetResult extends BaseResult {

	private byte[] value;
	private int ttl;

	public GetResult(byte[] key, byte[] value, int ttl) {
		super(key, null);
		this.value = value;
		this.ttl = ttl;
	}

	public GetResult(byte[] key, byte[] value, int ttl, KVException e) {
		super(key, e);
		this.value = value;
		this.ttl = ttl;
	}

	public int getCounter() throws KVException {
		checkException();
		if (value == null || value.length != 4) {
			throw new KVException("The key:" + Arrays.toString(key) + " is not a valid counter");
		}
		return KeyValueUtil.bytesToInt(value);
	}

	public int getTtl() {
		return ttl;
	}

	public byte[] getValue() throws KVException {
		checkException();
		return value;
	}
}
