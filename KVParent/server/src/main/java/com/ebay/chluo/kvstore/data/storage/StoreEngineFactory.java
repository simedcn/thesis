package com.ebay.chluo.kvstore.data.storage;

public class StoreEngineFactory {

	private static StoreEngineFactory instance;

	private StoreEngineFactory() {
		
	}

	public static StoreEngineFactory getInstance() {
		if (instance == null) {
			instance = new StoreEngineFactory();
		}
		return instance;
	}

	public IStoreEngine getPersistentStore(int cacheLimit) {
		return new StoreEngineProxy(new PersistentStoreEngine(cacheLimit));
	}

	public IStoreEngine getMemoryStore(int cacheLimit) {
		return new StoreEngineProxy(new MemoryStoreEngine(cacheLimit));
	}
}
 