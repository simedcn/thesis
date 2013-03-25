package com.ebay.kvstore.client;

/**
 * Used for asynchronous call
 * 
 * @author luochen
 * 
 */
public interface IClientHandler {

	public void onDelete(boolean success, byte[] key);

	public void onGet(byte[] key, byte[] value);

	public void onIncr(byte[] key, int value);

	public void onStat();

}
