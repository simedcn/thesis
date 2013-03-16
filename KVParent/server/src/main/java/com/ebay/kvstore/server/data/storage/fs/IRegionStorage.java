package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;

import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public interface IRegionStorage {

	public KeyValue getFromBuffer(byte[] key);

	public KeyValue[] getFromDisk(byte[] key) throws IOException;

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

	public void reset();

	public String getBaseDir();

	public KeyValueCache getBuffer();

	public void setBuffer(KeyValueCache buffer);

	public String getDataFile();

	public Region getRegion();

	public void setDataFile(String file) throws IOException;

	/**
	 * Create a new logger, and close the old one
	 * 
	 * @param newLogFile
	 * @throws IOException
	 */
	public void newLogger(String file) throws IOException;

	public void closeLogger() ;

	public void dispose();

}
