package com.ebay.kvstore.server.data.cache;

public interface ICacheReplacer {

	public byte[] getReplacement();

	public void reIndex(byte[] key);

	public void addIndex(byte[] key);

	public void deleteIndex(byte[] key);

	public static String LRU = "lru";

	public static String RANDOM = "random";

	public static String FIFO = "fifo";

}
