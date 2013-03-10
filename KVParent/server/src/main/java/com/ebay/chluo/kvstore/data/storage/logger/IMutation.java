package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;

/**
 * Used to define the mutation
 * 
 * @author luochen
 * 
 */
public interface IMutation {

	public static byte Set = 0;

	public static byte Delete = 1;

	public byte getType();

	public byte[] getKey();

	public byte[] getValue();

	void writeToExternal(LoggerOutputStream out) throws IOException;

	void readFromExternal(LoggerInputStream in) throws IOException;

}
