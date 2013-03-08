package com.ebay.chluo.kvstore.data.storage.cache;

public interface ICacheReplacer {

	public byte[] getReplacement();

	public void reIndex(byte[] key);

	public void addIndex(byte[] key);

	public void deleteIndex(byte[] key);

}
