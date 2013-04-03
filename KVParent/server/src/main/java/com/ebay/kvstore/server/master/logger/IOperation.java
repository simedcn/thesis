package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Address;

public interface IOperation {
	public static byte Load = 0;

	public static byte Unload = 1;

	public static byte Split = 2;

	public Address getAddr();

	public int getRegionId();

	public byte getType();

	public void readFromExternal(ILoggerInputStream in) throws IOException;

	public void writeToExternal(ILoggerOutputStream out) throws IOException;
}
