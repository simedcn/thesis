package com.ebay.kvstore.server.data.logger;

import com.ebay.kvstore.server.logger.ILogEntry;
import com.ebay.kvstore.structure.Value;

/**
 * Used to define the mutation
 * 
 * @author luochen
 * 
 */
public interface IMutation extends ILogEntry{

	public static byte Set = 0;

	public static byte Delete = 1;

	public byte[] getKey();
	
	public Value getValue();


}
