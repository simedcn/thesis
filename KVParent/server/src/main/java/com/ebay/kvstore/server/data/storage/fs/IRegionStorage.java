package com.ebay.kvstore.server.data.storage.fs;

import java.io.IOException;

import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public interface IRegionStorage {

	public void closeLogger();

	public void deleteFromBuffer(byte[] key);

	public void dispose();

	public String getRegionDir();

	public KeyValueCache getBuffer();

	public long getBufferLimit();

	public long getBufferUsed();

	public String getDataFile();

	public KeyValue getFromBuffer(byte[] key);

	public KeyValue[] getFromDisk(byte[] key) throws IOException;

	public Region getRegion();

	public void newLogger(String file) throws IOException;

	public void clear();

	public void clearBuffer();

	public void setBuffer(KeyValueCache buffer);

	public void setBufferLimit(int limit);

	public void setDataFile(String file) throws IOException;

	public void setLogger(String file) throws IOException;

	public void stat() throws IOException;

	public void storeInBuffer(byte[] key, Value value);

}
