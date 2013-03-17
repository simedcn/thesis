package com.ebay.kvstore;

/**
 * compare the given range with a given key(byte[])
 * 
 * @author luochen
 * 
 */
public interface KeyComparable {
	public int compareTo(byte[] key);
}
