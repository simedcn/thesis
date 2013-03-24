package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;

import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;

public interface IRegionStorage {

	public void closeLogger();

	public void deleteFromBuffer(byte[] key);

	public void dispose();

	public void flush();

	public String getBaseDir();

	public KeyValueCache getBuffer();

	public long getBufferLimit();

	public long getBufferUsed();

	public String getDataFile();

	public KeyValue getFromBuffer(byte[] key);

	public KeyValue[] getFromDisk(byte[] key) throws IOException;

	public Region getRegion();

	/**
	 * Create a new logger, and close the old one
	 * 
	 * @param newLogFile
	 * @throws IOException
	 */
	public void newLogger(String file) throws IOException;

	public void reset();

	public void resetBuffer();

	public void setBuffer(KeyValueCache buffer);

	public void setBufferLimit(int limit);

	public void setDataFile(String file) throws IOException;

	public void setLogger(String file) throws IOException;

	public void stat() throws IOException;

	/**
	 * 1.put the key/value into buffer, and 2.write redo log
	 * 
	 * @param key
	 * @param value
	 */
	public void storeInBuffer(byte[] key, byte[] value);

}
