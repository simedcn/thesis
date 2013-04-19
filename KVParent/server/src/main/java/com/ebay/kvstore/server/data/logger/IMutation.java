package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Value;

/**
 * Used to define the mutation
 * 
 * @author luochen
 * 
 */
public interface IMutation {

	public static byte Set = 0;

	public static byte Delete = 1;

	public byte[] getKey();

	public byte getType();

	public Value getValue();

	void readFromExternal(ILoggerInputStream in) throws IOException;

	void writeToExternal(ILoggerOutputStream out) throws IOException;

}
