package com.ebay.chluo.kvstore.data.storage;

import com.ebay.chluo.kvstore.data.storage.cache.KeyValueCache;
import com.ebay.chluo.kvstore.data.storage.logger.DeleteMutation;
import com.ebay.chluo.kvstore.data.storage.logger.IRedoLogger;
import com.ebay.chluo.kvstore.data.storage.logger.SetMutation;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;

/**
 * Used for manage region data file in HDFS
 * 
 * @author luochen
 * 
 */
public class FileRegionStorage implements IRegionStorage {

	protected IRedoLogger logger;

	protected KeyValueCache buffer;

	protected long bufferLimit;

	public FileRegionStorage(int bufferLimit) {
		this.bufferLimit = bufferLimit;
		// The KeyValue cache will not replace entries automatically, in
		// contrast, we will check the buffer size manually.
		this.buffer = new KeyValueCache(0, null);
	}

	@Override
	public void load() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBufferLimit(int limit) {
		this.bufferLimit = limit;
	}

	@Override
	public long getBufferLimit() {
		return bufferLimit;
	}

	@Override
	public KeyValue getFromBuffer(byte[] key) {
		return buffer.get(key);
	}

	@Override
	public KeyValue[] getFromDisk(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeInBuffer(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		buffer.set(key, value);
		logger.write(new SetMutation(key, value));

		if (buffer.getUsed() > bufferLimit) {
			commit();
		}
	}

	@Override
	public void deleteFromBuffer(byte[] key) {
		// TODO Auto-generated method stub
		buffer.set(key, new Value(null, true));
		logger.write(new DeleteMutation(key));

	}

	@Override
	public long getBufferUsed() {
		return buffer.getUsed();
	}

}
