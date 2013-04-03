package com.ebay.kvstore.client;

import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.RegionTable;

public interface IKVClient {

	public void delete(byte[] key) throws KVException;

	public byte[] get(byte[] key) throws KVException;

	public void set(byte[] key, byte[] value) throws KVException;

	public IKVClientHandler getClientHandler();

	public ClientOption getClientOption();

	public int getCounter(byte[] key) throws KVException;

	public int incr(byte[] key, int intial, int incremental) throws KVException;

	public void setHandler(IKVClientHandler handler);

	public DataServerStruct[] stat() throws KVException;

	public void close();

	public void updateRegionTable() throws KVException;

	public void setRegionTable(RegionTable table);

}
