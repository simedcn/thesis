package com.ebay.kvstore.server.data.storage;

import java.io.IOException;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.structure.Region;

public class StoreEngineFactory {

	private static StoreEngineFactory instance;

	public static StoreEngineFactory getInstance() {
		if (instance == null) {
			instance = new StoreEngineFactory();
		}
		return instance;
	}

	private StoreEngineFactory() {

	}

	public IStoreEngine getMemoryStore(IConfiguration conf, Region... regions) throws IOException {
		return new StoreEngineProxy(new MemoryStoreEngine(conf, regions));
	}

	public IStoreEngine getPersistentStore(IConfiguration conf, Region... regions)
			throws IOException {
		return new StoreEngineProxy(new PersistentStoreEngine(conf, regions));
	}
}
