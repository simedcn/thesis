package com.ebay.chluo.kvstore.data.storage;

import com.ebay.chluo.kvstore.data.storage.logger.IRedoLogger;
import com.ebay.chluo.kvstore.structure.KeyValue;

/**
 * Used for manage region data file in HDFS
 * 
 * @author luochen
 * 
 */
public class FileRegionStorage implements IRegionStorage {

	protected IRedoLogger logger;

	protected KeyValueCache buffer;

	protected int bufferLimit;

	// cache for all key/values
	protected KeyValueCache cache;

	public FileRegionStorage(int bufferLimit, KeyValueCache cache) {
		this.bufferLimit = bufferLimit;
		this.cache = cache;
	}

	@Override
	public void load() {
		// TODO Auto-generated method stub

	}

	@Override
	public void set(byte[] key, byte value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void incr(byte[] key, int incremental, int initValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public KeyValue get(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue delete(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBufferLimit(int limit) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getBufferLimit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

}
