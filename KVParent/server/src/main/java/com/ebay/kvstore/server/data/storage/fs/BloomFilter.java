package com.ebay.kvstore.server.data.storage.fs;

import java.util.BitSet;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.InvalidConfException;

public class BloomFilter {
	private BitSet bits;

	private int size;

	public BloomFilter(IConfiguration conf) {
		size = conf.getInt(IConfigurationKey.Dataserver_Region_Bloomfilter_Size) * 8;
		if (size <= 0) {
			throw new InvalidConfException(IConfigurationKey.Dataserver_Region_Bloomfilter_Size,
					"positive number",
					conf.get(IConfigurationKey.Dataserver_Region_Bloomfilter_Size));
		}
		this.bits = new BitSet(size);
	}

	public BloomFilter(int size) {
		this.size = size;
		this.bits = new BitSet(size);
	}

	public boolean get(byte[] key) {
		int h1 = hashCode(key, 17);
		int h2 = hashCode(key, 23);
		int h3 = hashCode(key, 31);
		int h4 = hashCode(key, 37);
		return bits.get(h1 % size) && bits.get(h2 % size) && bits.get(h3 % size)
				&& bits.get(h4 % size);
	}

	public void clear() {
		bits.clear();
	}

	public void set(byte[] key) {
		int h1 = hashCode(key, 17);
		int h2 = hashCode(key, 23);
		int h3 = hashCode(key, 31);
		int h4 = hashCode(key, 37);
		bits.set(h1 % size);
		bits.set(h2 % size);
		bits.set(h3 % size);
		bits.set(h4 % size);

	}

	private static int hashCode(byte[] key, int prime) {
		if (key == null)
			return 0;

		int result = 1;
		for (byte element : key)
			result = prime * result + element;
		if (result < 0) {
			result = -result;
		}
		return result;
	}

}
