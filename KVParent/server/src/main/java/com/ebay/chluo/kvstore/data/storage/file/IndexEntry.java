package com.ebay.chluo.kvstore.data.storage.file;

import java.util.Arrays;

import com.ebay.chluo.kvstore.KeyComparable;
import com.ebay.chluo.kvstore.KeyValueUtil;

public class IndexEntry implements Comparable<IndexEntry>, KeyComparable {
	byte[] keyStart;
	byte[] keyEnd;

	int blockStart;
	int blockEnd;

	int offset;

	public IndexEntry(byte[] keyStart, byte[] keyEnd, int blockStart, int blockEnd, int offset) {
		super();
		this.keyStart = keyStart;
		this.keyEnd = keyEnd;
		this.blockStart = blockStart;
		this.blockEnd = blockEnd;
		this.offset = offset;
	}

	@Override
	public int compareTo(IndexEntry e) {
		int e1 = KeyValueUtil.compare(keyStart, e.keyEnd);
		int e2 = KeyValueUtil.compare(keyEnd, e.keyEnd);
		if (e1 > 0) {
			return 1;
		} else if (e2 < 0) {
			return -1;
		} else if (e1 == 0 && e2 == 0) {
			return 0;
		} else {
			// The two regions are overlapped, should not happen
			throw new IllegalArgumentException("The two regions are overlapped:" + this.toString()
					+ " " + e.toString());
		}
	}

	@Override
	public int compareTo(byte[] key) {
		int e1 = KeyValueUtil.compare(key, keyStart);
		int e2 = KeyValueUtil.compare(key, keyEnd);
		if (e1 >= 0 && e2 <= 0) {
			return 0;
		} else if (e1 < 0) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public String toString() {
		return "IndexEntry [keyStart=" + Arrays.toString(keyStart) + ", keyEnd="
				+ Arrays.toString(keyEnd) + ", blockStart=" + blockStart + ", blockEnd=" + blockEnd
				+ ", offset=" + offset + "]";
	}

}
