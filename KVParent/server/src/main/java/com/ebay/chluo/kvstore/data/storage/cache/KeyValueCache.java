package com.ebay.chluo.kvstore.data.storage.cache;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ebay.chluo.kvstore.ByteArrayComparator;
import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;
import com.ebay.chluo.kvstore.structure.Value;

public class KeyValueCache implements Iterable<Entry<byte[], Value>> {

	protected volatile SortedMap<byte[], Value> cache;

	protected long limit;

	protected volatile long used;

	protected ICacheReplacer replacer;

	public KeyValueCache(int limit, ICacheReplacer replacer) {
		this.cache = new TreeMap<>(new ByteArrayComparator());
		this.limit = limit;
		this.used = 0;
		this.replacer = replacer;
	}

	public KeyValue get(byte[] key) {
		Value v = cache.get(key);
		if (v != null && replacer != null) {
			replacer.reIndex(key);
		}
		return new KeyValue(key, v);

	}

	public void set(byte[] key, Value value) {
		synchronized (key) {
			Value v = cache.get(key);
			if (v == null) {
				v = value;
				cache.put(key, v);
				used = used + key.length + v.getSize();
				if (replacer != null) {
					replacer.addIndex(key);
				}
			} else {
				used = used - v.getSize() + value.getSize();
				if (replacer != null) {
					replacer.reIndex(key);
				}
			}
		}
		checkMemory();
	}

	public void set(byte[] key, byte[] value) {
		synchronized (key) {
			Value v = cache.get(key);
			if (v == null) {
				v = new Value(value);
				cache.put(key, v);
				used = used + key.length + v.getSize();
				if (replacer != null) {
					replacer.addIndex(key);
				}
			} else {
				byte[] old = v.getValue();
				v.setValue(value);
				used = used - old.length + value.length;
				if (replacer != null) {
					replacer.reIndex(key);
				}
			}
			checkMemory();
		}
	}

	public KeyValue delete(byte[] key) {
		synchronized (key) {
			Value v = cache.remove(key);
			if (v != null) {
				used = used - key.length - v.getSize();
				if (replacer != null) {
					replacer.deleteIndex(key);
				}
			}
			return new KeyValue(key, v);
		}
	}

	public void addAll(KeyValueCache buffer) {
		this.cache.putAll(buffer.cache);
	}

	public KeyValue incr(byte[] key, int incremental, int initValue) {
		synchronized (key) {
			Value v = cache.get(key);
			if (v == null) {
				v = new Value(KeyValueUtil.intToBytes(initValue + incremental));
				cache.put(key, v);
				used = used + key.length + v.getSize();
				if (replacer != null) {
					replacer.addIndex(key);
				}
			} else {
				v.incr(incremental);
				if (replacer != null) {
					replacer.reIndex(key);
				}
			}
			return new KeyValue(key, v);
		}
	}

	public long getUsed() {
		return used;
	}

	/**
	 * Check the memory usage after set operator, and will remove some
	 * key/values based on some algorithms
	 */
	private void checkMemory() {
		if (limit <= 0) {
			return;
		}
		if (used > limit) {
			synchronized (cache) {
				while (used > limit) {
					byte[] key = replacer.getReplacement();
					Value v = cache.remove(key);
					used = used - key.length - v.getSize();
				}
			}
		}
	}

	@Override
	public Iterator<Entry<byte[], Value>> iterator() {
		return cache.entrySet().iterator();
	}
}
