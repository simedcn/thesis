package com.ebay.kvstore.server.master.logger;

import com.ebay.kvstore.server.logger.ILogEntry;
import com.ebay.kvstore.structure.Address;

public interface IOperation extends ILogEntry {
	public static byte Load = 0;

	public static byte Unload = 1;

	public static byte Split = 2;

	public static byte Merge = 3;

	public Address getAddr();

	public int getRegionId();

}
