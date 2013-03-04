package com.ebay.chluo.kvstore.data.storage;

import com.ebay.chluo.kvstore.structure.KeyValue;

public interface IRegionStorage {

	public void load();

	public void set(byte[] key, byte value);

	public void incr(byte[] key, int incremental, int initValue);

	public KeyValue get(byte[] key);

	public KeyValue delete(byte[] key);

	public void setBufferLimit(int limit);

	public void getBufferLimit();

	public void flush();
}
