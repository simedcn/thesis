package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Used to define the mutation
 * @author luochen
 *
 */
public interface IMutation {

	public static int Set = 0;
	
	public static int Delete = 1;
	
	public int getType();

	public void writeToExternal(OutputStream out);

	public void readFromExternal(InputStream in);
}
