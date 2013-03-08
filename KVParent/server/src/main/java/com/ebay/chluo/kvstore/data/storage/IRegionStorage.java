package com.ebay.chluo.kvstore.data.storage;

import com.ebay.chluo.kvstore.structure.KeyValue;

public interface IRegionStorage {

	public void load();

	public KeyValue getFromBuffer(byte[] key);

	public KeyValue[] getFromDisk(byte[] key);

	/**
	 * 1.put the key/value into buffer, and 2.write redo log
	 * 
	 * @param key
	 * @param value
	 */
	public void storeInBuffer(byte[] key, byte[] value);

	public void setBufferLimit(int limit);

	public void deleteFromBuffer(byte[] key);

	public long getBufferLimit();

	public void commit();

	public long getBufferUsed();
}
