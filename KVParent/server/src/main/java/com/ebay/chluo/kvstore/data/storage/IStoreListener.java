package com.ebay.chluo.kvstore.data.storage;

import com.ebay.chluo.kvstore.structure.Value;

public interface IStoreListener {
	public void onSet(byte[] key, byte[] value);

	public void onGet(byte[] key, Value value);

	public void onDelete(byte[] key);

	public void onIncr(byte[] key, int value);
}
