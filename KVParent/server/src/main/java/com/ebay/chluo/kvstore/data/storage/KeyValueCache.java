package com.ebay.chluo.kvstore.data.storage;

import java.util.HashMap;
import java.util.Map;

import com.ebay.chluo.kvstore.structure.Value;

public class KeyValueCache {

	protected Map<byte[], Value> cache;

	protected int limit;

	public KeyValueCache(int limit) {
		this.cache = new HashMap<>();
		this.limit = limit;
	}

	public Value get(byte[] key) {
		return null;
	}

	public void set(byte[] key, byte[] value) {

	}

	public void getSize() {

	}
}
