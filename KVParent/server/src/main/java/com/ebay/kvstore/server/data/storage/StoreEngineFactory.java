package com.ebay.kvstore.server.data.storage;

import java.io.IOException;

import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.server.conf.InvalidConfException;
import com.ebay.kvstore.structure.Region;

public class StoreEngineFactory {
	public static IStoreEngine createStoreEngine(IConfiguration conf, Region... regions)
			throws IOException {
		String type = conf.get(IConfigurationKey.Storage_Policy);
		IStoreEngine engine = null;
		switch (type) {
		case "persistent":
			engine = new StoreEngineProxy(new PersistentStoreEngine(conf, regions));
			break;
		case "memory":
			engine = new StoreEngineProxy(new MemoryStoreEngine(conf, regions));
			break;
		default:
			throw new InvalidConfException(IConfigurationKey.Storage_Policy, "persistent|memory",
					type);
		}
		return engine;
	}
}
