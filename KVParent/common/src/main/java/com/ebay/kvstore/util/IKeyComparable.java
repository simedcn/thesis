package com.ebay.kvstore.util;

/**
 * compare the given range with a given key(byte[])
 * 
 * @author luochen
 * 
 */
public interface IKeyComparable {
	public int compareTo(byte[] key);
}
