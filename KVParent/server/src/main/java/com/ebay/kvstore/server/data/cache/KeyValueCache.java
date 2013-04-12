package com.ebay.kvstore.server.data.cache;

import static com.ebay.kvstore.KeyValueUtil.getKeyValueLen;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.structure.KeyValue;
import com.ebay.kvstore.structure.Value;

public class KeyValueCache implements Iterable<Entry<byte[], Value>> {

	public static KeyValueCache forBuffer() {
		return new KeyValueCache(-1, null);
	}

	public static KeyValueCache forCache(int limit, ICacheReplacer replacer) {
		return new KeyValueCache(limit, replacer);
	}

	public static KeyValueCache forCache(int limit, String replacer) {
		ICacheReplacer r = CacheReplacerFactory.createReplacer(replacer);
		return forCache(limit, r);
	}

	protected volatile SortedMap<byte[], Value> cache;

	protected long limit;

	protected volatile long used;

	protected ICacheReplacer replacer;

	protected ReadWriteLock lock = new ReentrantReadWriteLock();

	protected Lock readLock = lock.readLock();

	protected Lock writeLock = lock.writeLock();

	protected KeyValueCache(int limit, ICacheReplacer replacer) {
		this.cache = Collections.synchronizedSortedMap(new TreeMap<byte[], Value>(
				new ByteArrayComparator()));
		this.limit = limit;
		this.used = 0;
		this.replacer = replacer;
	}

	public void addAll(KeyValueCache buffer) {
		if (buffer == null) {
			return;
		}
		try {
			writeLock.lock();
			for (Entry<byte[], Value> e : buffer) {
				Value v = cache.get(e.getKey());
				cache.put(e.getKey(), e.getValue());
				if (v == null) {
					onSet(e.getKey(), e.getValue());
				} else {
					onUpdate(e.getKey(), v, e.getValue());
				}
			}
			checkMemory();
		} finally {
			writeLock.unlock();
		}
	}

	public KeyValue delete(byte[] key) {
		try {
			writeLock.lock();
			Value v = cache.remove(key);
			if (v != null) {
				onDelete(key, v);
			}
			return new KeyValue(key, v);
		} finally {
			writeLock.unlock();
		}

	}

	public KeyValue get(byte[] key) {
		try {
			readLock.lock();
			Value v = cache.get(key);
			onGet(key, v);
			if (v != null) {
				return new KeyValue(key, v);
			} else {
				return null;
			}
		} finally {
			readLock.unlock();
		}

	}

	public Lock getReadLock() {
		return readLock;
	}

	public long getUsed() {
		return used;
	}

	public Lock getWriteLock() {
		return writeLock;
	}

	public KeyValue incr(byte[] key, int incremental, int initValue) {
		try {
			writeLock.lock();
			Value v = cache.get(key);
			if (v == null) {
				v = new Value(KeyValueUtil.intToBytes(initValue + incremental));
				cache.put(key, v);
				onSet(key, v);
			} else {
				v.incr(incremental);
			}
			return new KeyValue(key, v);
		} finally {
			writeLock.unlock();
		}

	}

	@Override
	public Iterator<Entry<byte[], Value>> iterator() {
		return cache.entrySet().iterator();
	}

	public void remove(byte[] start, byte[] end) {
		try {
			writeLock.lock();
			Entry<byte[], Value> e = null;
			Iterator<Entry<byte[], Value>> it = cache.entrySet().iterator();
			while (it.hasNext()) {
				e = it.next();
				if (KeyValueUtil.inRange(e.getKey(), start, end)) {
					it.remove();
					onDelete(e.getKey(), e.getValue());
				}
			}
		} finally {
			writeLock.unlock();
		}

	}

	public void removeAll(KeyValueCache buffer) {
		try {
			writeLock.lock();
			for (Entry<byte[], Value> e : buffer) {
				Value v = cache.remove(e.getKey());
				if (v != null) {
					onDelete(e.getKey(), v);
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void reset() {
		used = 0;
		cache.clear();
	}

	public void set(byte[] key, byte[] value) {
		set(key, new Value(value));
	}

	public void set(byte[] key, Value value) {
		try {
			writeLock.lock();
			Value v = cache.get(key);
			if (v == null) {
				onSet(key, value);
			} else {
				onUpdate(key, v, value);
			}
			cache.put(key, value);
			checkMemory();
		} finally {
			writeLock.unlock();
		}
	}

	public void setLimit(int cacheLimit) {
		this.limit = cacheLimit;
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
			while (used > limit) {
				byte[] key = replacer.getReplacement();
				Value v = cache.remove(key);
				onDelete(key, v);
			}
		}
	}

	private void onDelete(byte[] key, Value value) {
		if (value != null) {
			used = used - getKeyValueLen(key, value);
			if (replacer != null) {
				replacer.deleteIndex(key);
			}
		}
	}

	private void onGet(byte[] key, Value value) {
		if (value != null && replacer != null) {
			replacer.reIndex(key);
		}
	}

	private void onSet(byte[] key, Value value) {
		used = used + getKeyValueLen(key, value);
		if (replacer != null) {
			replacer.addIndex(key);
		}
	}

	private void onUpdate(byte[] key, Value oldValue, Value newValue) {
		used = used - getKeyValueLen(key, oldValue) + getKeyValueLen(key, newValue);
		if (replacer != null) {
			replacer.reIndex(key);
		}
	}
	private class ByteArrayComparator implements Comparator<byte[]> {

		@Override
		public int compare(byte[] key1, byte[] key2) {
			return KeyValueUtil.compare(key1, key2);
		}
	}

	
}
