package com.ebay.chluo.kvstore.data.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.chluo.kvstore.data.storage.logger.IMutation;
import com.ebay.chluo.kvstore.data.storage.logger.IRedoLogger;
import com.ebay.chluo.kvstore.data.storage.logger.LoggerFSInputIterator;
import com.ebay.chluo.kvstore.data.storage.logger.SetMutation;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Region;

public class MemoryStoreEngine extends BaseStoreEngine {

	private static Logger logger = LoggerFactory.getLogger(MemoryStoreEngine.class);

	protected Map<Region, IRedoLogger> loggers;

	public MemoryStoreEngine(int limit) {
		super(limit);
		loggers = new HashMap<Region, IRedoLogger>();
	}

	@Override
	public void set(byte[] key, byte[] value) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IRedoLogger logger = getRedoLogger(region);
		cache.set(key, value);
		logger.write(new SetMutation(key, value));
	}

	@Override
	public KeyValue get(byte[] key) throws InvalidKeyException {
		checkKeyRegion(key);
		KeyValue kv = cache.get(key);
		return kv;
	}

	@Override
	public KeyValue incr(byte[] key, int incremental, int initValue) throws InvalidKeyException {
		Region region = checkKeyRegion(key);
		IRedoLogger logger = getRedoLogger(region);
		KeyValue kv = cache.incr(key, incremental, initValue);
		logger.write(new SetMutation(key, kv.getValue().getValue()));
		return kv;
	}

	@Override
	public void delete(byte[] key) throws InvalidKeyException {
		checkKeyRegion(key);
		cache.delete(key);
	}

	@Override
	public void flush() {
		// do nothing
	}

	@Override
	public void unloadRegion(int regionId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void loadRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public void splitRegion() {
		// TODO Auto-generated method stub

	}

	@Override
	public long getMemoryUsed() {
		return cache.getUsed();
	}

	private IRedoLogger getRedoLogger(Region region) {
		return loggers.get(region);
	}

	protected synchronized void loadLogger(String file) {
		try {
			LoggerFSInputIterator it = new LoggerFSInputIterator(file);
			while (it.hasNext()) {
				IMutation mutation = it.next();
				switch (mutation.getType()) {
				case IMutation.Set:
					cache.set(mutation.getKey(), mutation.getValue());
					break;
				case IMutation.Delete:
					cache.delete(mutation.getKey());
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Error occured when loading the log file:" + file, e);
		}
	}

}
