package com.ebay.kvstore.server.data.storage;

import com.ebay.kvstore.structure.Value;

public interface IStoreListener {
	public void onSet(byte[] key, byte[] value);

	public void onGet(byte[] key, Value value);

	public void onDelete(byte[] key);

	public void onIncr(byte[] key, int value);
}
