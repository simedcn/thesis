package com.ebay.kvstore.server.data.storage.helper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.kvstore.KeyValueUtil;
import com.ebay.kvstore.server.data.storage.fs.IBlockOutputStream;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

/**
 * The set of classes is used for flushing region storage data to file
 * 
 * @author luochen
 * 
 */
public abstract class BaseHelper implements Runnable {
	protected IRegionStorage storage;
	protected IConfiguration conf;

	public BaseHelper(IRegionStorage storage, IConfiguration conf) {
		super();
		this.storage = storage;
		this.conf = conf;
	}

	protected void flushCache(Iterator<Entry<byte[], Value>> it, IBlockOutputStream out)
			throws IOException {
		Entry<byte[], Value> e = null;
		while (it.hasNext()) {
			e = it.next();
			if (!e.getValue().isDeleted()) {
				KeyValueUtil.writeToExternal(out, new KeyValue(e.getKey(), e.getValue()));
			}
		}
	}

	protected void flushFile(Iterator<KeyValue> it, IBlockOutputStream out) throws IOException {
		KeyValue kv = null;
		while (it.hasNext()) {
			kv = it.next();
			KeyValueUtil.writeToExternal(out, kv);
		}
	}

	@SuppressWarnings("rawtypes")
	protected Object nextEntry(Iterator it) {
		if (it.hasNext()) {
			return it.next();
		} else {
			return null;
		}
	}
}
