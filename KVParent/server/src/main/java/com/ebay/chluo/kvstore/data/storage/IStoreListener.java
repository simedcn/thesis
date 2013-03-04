package com.ebay.chluo.kvstore.data.storage;

public interface IStoreListener {
	public void onSet(byte[] key, byte[] value);

	public void onGet(byte[] key, byte[] value);

	public void onDelete(byte[] key, byte[] oldValue);

	public void onIncr(byte[] key, int value, int incremental);
}
