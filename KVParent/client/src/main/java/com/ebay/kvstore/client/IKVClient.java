package com.ebay.kvstore.client;

import com.ebay.kvstore.client.result.GetResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.RegionTable;

public interface IKVClient {

	public void delete(byte[] key) throws KVException;

	public GetResult get(byte[] key) throws KVException;

	public void set(byte[] key, byte[] value) throws KVException;

	public void set(byte[] key, byte[] value, int ttl) throws KVException;

	public IKVClientHandler getClientHandler();

	public ClientOption getClientOption();

	public GetResult getCounter(byte[] key) throws KVException;

	public int incr(byte[] key, int incremental, int initValue) throws KVException;

	public int incr(byte[] key, int incremental, int initValue, int ttl) throws KVException;

	public void setHandler(IKVClientHandler handler);

	public DataServerStruct[] stat() throws KVException;

	public void close();

	public void updateRegionTable() throws KVException;

	public void setRegionTable(RegionTable table);

}
