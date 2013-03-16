package com.ebay.kvstore.server.data.logger;

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

	void writeToExternal(ILoggerOutputStream out) throws IOException;

	void readFromExternal(ILoggerInputStream in) throws IOException;

}
