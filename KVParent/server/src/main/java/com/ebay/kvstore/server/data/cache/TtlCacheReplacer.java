package com.ebay.kvstore.server.data.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.ebay.kvstore.KeyValueUtil;

public class TtlCacheReplacer extends BaseCacheReplacer {

	private SortedSet<TtlEntry> index;
	private Map<byte[], TtlEntry> subIndex;

	public TtlCacheReplacer() {
		index = new TreeSet<>();
		subIndex = new HashMap<>();
	}

	@Override
	public void addIndex(byte[] key, long expire) {
		TtlEntry entry = new TtlEntry(key, expire);
		index.add(entry);
		subIndex.put(key, entry);
	}

	@Override
	public void deleteIndex(byte[] key) {
		TtlEntry entry = subIndex.get(key);
		if (entry != null) {
			index.remove(entry);
		}
	}

	@Override
	public byte[] getReplacement() {
		byte[] result = null;
		Iterator<TtlEntry> it = index.iterator();
		if (it.hasNext()) {
			result = it.next().key;
			subIndex.remove(result);
			it.remove();
		}
		return result;
	}

	@Override
	public void reIndex(byte[] key) {

	}

	private class TtlEntry implements Comparable<TtlEntry> {
		byte[] key;
		long expire;

		public TtlEntry(byte[] key, long expire) {
			this.key = key;
			this.expire = expire;
		}

		@Override
		public int compareTo(TtlEntry o) {
			byte[] key1 = this.key;
			byte[] key2 = o.key;
			long expire1 = this.expire;
			long expire2 = o.expire;
			if (expire1 == 0) {
				if (expire2 > 0) {
					return -1;
				} else {
					return KeyValueUtil.compare(key1, key2);
				}
			} else if (expire2 == 0) {
				return 1;
			} else {
				if (expire1 != expire2) {
					return Long.compare(expire1, expire2);
				} else {
					return KeyValueUtil.compare(key1, key2);
				}
			}
		}

	}

}
