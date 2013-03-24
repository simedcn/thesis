package com.ebay.kvstore.server.data.cache;

import java.util.LinkedList;
import java.util.List;

public class LRUCacheReplacer extends BaseCacheReplacer {

	protected List<byte[]> index;

	public LRUCacheReplacer() {
		index = new LinkedList<>();
	}

	@Override
	public void addIndex(byte[] key) {
		index.add(key);
	}

	@Override
	public void deleteIndex(byte[] key) {
		index.remove(key);
	}

	@Override
	public byte[] getReplacement() {
		if (index.size() > 0) {
			return index.remove(0);
		} else {
			return null;
		}
	}

	@Override
	public void reIndex(byte[] key) {
		index.remove(key);
		index.add(key);
	}

	/*
	 * private static class Entry implements Comparable<Entry> { byte[] key;
	 * long time;
	 * 
	 * public Entry(byte[] key, long time) { super(); this.key = key; this.time
	 * = time; }
	 * 
	 * @Override public int compareTo(Entry o) { return Long.compare(time,
	 * o.time); }
	 * 
	 * @Override public int hashCode() { final int prime = 31; int result = 1;
	 * result = prime * result + Arrays.hashCode(key); return result; }
	 * 
	 * @Override public boolean equals(Object obj) { if (this == obj) return
	 * true; if (obj == null) return false; if (getClass() != obj.getClass())
	 * return false; Entry other = (Entry) obj; return Arrays.equals(key,
	 * other.key); }
	 * 
	 * @Override public String toString() { return "Entry [key=" +
	 * Arrays.toString(key) + ", time=" + time + "]"; }
	 * 
	 * }
	 */
}
